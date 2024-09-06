"""Amazon Web Services."""
import enum
import fnmatch
import functools
import json
import os
import re
import subprocess
import time
import typing
from typing import Any, Dict, Iterator, List, Optional, Tuple

from sky import clouds
from sky import exceptions
from sky import provision as provision_lib
from sky import sky_logging
from sky import skypilot_config
from sky.adaptors import aws
from sky.clouds import service_catalog
from sky.skylet import constants
from sky.utils import common_utils
from sky.utils import resources_utils
from sky.utils import rich_utils
from sky.utils import subprocess_utils
from sky.utils import ux_utils

if typing.TYPE_CHECKING:
    # renaming to avoid shadowing variables
    from sky import resources as resources_lib
    from sky import status_lib

logger = sky_logging.init_logger(__name__)

# This local file (under ~/.aws/) will be uploaded to remote nodes (any
# cloud), if all of the following conditions hold:
#   - the current user identity is not using AWS SSO
#   - this file exists
# It has the following purposes:
#   - make all nodes (any cloud) able to access private S3 buckets
#   - make some remote nodes able to launch new nodes on AWS (i.e., makes
#     AWS head node able to launch AWS workers, or any-cloud jobs controller
#     able to launch spot clusters on AWS).
#
# If we detect the current user identity is AWS SSO, we will not upload this
# file to any remote nodes (any cloud). Instead, a SkyPilot IAM role is
# assigned to both AWS head and workers.
# TODO(skypilot): This also means we leave open a bug for AWS SSO users that
# use multiple clouds. The non-AWS nodes will have neither the credential
# file nor the ability to understand AWS IAM.
_CREDENTIAL_FILES = [
    'credentials',
]

DEFAULT_AMI_GB = 45

# Temporary measure, as deleting per-cluster SGs is too slow.
# See https://github.com/skypilot-org/skypilot/pull/742.
# Generate the name of the security group we're looking for.
# (username, last 4 chars of hash of hostname): for uniquefying
# users on shared-account scenarios.
DEFAULT_SECURITY_GROUP_NAME = f'sky-sg-{common_utils.user_and_hostname_hash()}'
# Security group to use when user specified ports in their resources.
USER_PORTS_SECURITY_GROUP_NAME = 'sky-sg-{}'

KOMODO_SKYPILOT_AMIS = {
    'ap-northeast-1': 'ami-01fd2743ebe2e5d29',
    'ap-northeast-2': 'ami-03faad73854d4dc4c',
    'ap-northeast-3': 'ami-0e06ad065d2cb70e0',
    'ap-south-1': 'ami-06aa43a5dff6b7ff0',
    'ap-southeast-1': 'ami-067de2aa1a6fba927',
    'ap-southeast-2': 'ami-0bdd4add0a958d6e1',
    'ca-central-1': 'ami-0880d13252fad1b67',
    'eu-central-1': 'ami-04b5edd2ed7ef149c',
    'eu-north-1': 'ami-05bc11fa9a8d72614',
    'eu-west-1': 'ami-03120c7b632ac91b1',
    'eu-west-2': 'ami-0642960ba679977ce',
    'eu-west-3': 'ami-020d44dff622b601c',
    'sa-east-1': 'ami-03aeb59d71a6bb1ca',
    'us-east-1': 'ami-0dbe09b43c2081684',
    'us-east-2': 'ami-00e8d1a1972fca577',
    'us-west-1': 'ami-07c19f3180452e458',
    'us-west-2': 'ami-015749b447aa499ae'
}


class AWSIdentityType(enum.Enum):
    """AWS identity type.

    The account type is determined by the current user identity, based on `aws
    configure list`. We will check the existence of the value in the output of
    `aws configure list` to determine the account type.
    """
    #       Name                    Value             Type    Location
    #       ----                    -----             ----    --------
    #    profile                     1234              env    ...
    # access_key     ****************abcd              sso
    # secret_key     ****************abcd              sso
    #     region                <not set>             None    None
    SSO = 'sso'

    ENV = 'env'

    IAM_ROLE = 'iam-role'

    CONTAINER_ROLE = 'container-role'
    ASSUME_ROLE = 'assume-role'

    ASSUME_ROLE_WITH_WEB_IDENTITY = 'assume-role-with-web-identity'

    #       Name                    Value             Type    Location
    #       ----                    -----             ----    --------
    #    profile                <not set>             None    None
    # access_key     ****************abcd shared-credentials-file
    # secret_key     ****************abcd shared-credentials-file
    #     region                us-east-1      config-file    ~/.aws/config
    SHARED_CREDENTIALS_FILE = 'shared-credentials-file'


@clouds.CLOUD_REGISTRY.register
class AWS(clouds.Cloud):
    """Amazon Web Services."""

    _REPR = 'AWS'

    # AWS has a limit of the tag value length to 256 characters.
    # By testing, the actual limit is 256 - 8 = 248 characters
    # (our provisioner adds additional `-worker`), due to the
    # maximum length of DescribeInstances API filter value.
    # Reference: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/Using_Tags.html # pylint: disable=line-too-long
    _MAX_CLUSTER_NAME_LEN_LIMIT = 248

    _SUPPORTS_SERVICE_ACCOUNT_ON_REMOTE = True

    _regions: List[clouds.Region] = []

    _INDENT_PREFIX = '    '
    _STATIC_CREDENTIAL_HELP_STR = (
        'Run the following commands:'
        f'\n{_INDENT_PREFIX}  $ aws configure'
        f'\n{_INDENT_PREFIX}  $ aws configure list  # Ensure that this shows identity is set.'
        f'\n{_INDENT_PREFIX}For more info: '
        'https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html'  # pylint: disable=line-too-long
    )

    _SUPPORTED_DISK_TIERS = set(resources_utils.DiskTier)
    PROVISIONER_VERSION = clouds.ProvisionerVersion.SKYPILOT
    STATUS_VERSION = clouds.StatusVersion.SKYPILOT

    @classmethod
    def _unsupported_features_for_resources(
        cls, resources: 'resources_lib.Resources'
    ) -> Dict[clouds.CloudImplementationFeatures, str]:
        if resources.use_spot:
            return {
                clouds.CloudImplementationFeatures.STOP:
                    ('Stopping spot instances is currently not supported on'
                     f' {cls._REPR}.'),
            }
        return {}

    @classmethod
    def max_cluster_name_length(cls) -> Optional[int]:
        return cls._MAX_CLUSTER_NAME_LEN_LIMIT

    @classmethod
    def _sso_credentials_help_str(cls, expired: bool = False) -> str:
        help_str = 'Run the following commands (must use AWS CLI v2):'
        if not expired:
            help_str += f'\n{cls._INDENT_PREFIX}  $ aws configure sso'
        help_str += (
            f'\n{cls._INDENT_PREFIX}  $ aws sso login --profile <profile_name>'
            f'\n{cls._INDENT_PREFIX}For more info: '
            'https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sso.html'  # pylint: disable=line-too-long
        )
        return help_str

    _MAX_AWSCLI_MAJOR_VERSION = 1

    #### Regions/Zones ####

    @classmethod
    def regions_with_offering(cls, instance_type: str,
                              accelerators: Optional[Dict[str, int]],
                              use_spot: bool, region: Optional[str],
                              zone: Optional[str]) -> List[clouds.Region]:
        del accelerators  # unused
        regions = service_catalog.get_region_zones_for_instance_type(
            instance_type, use_spot, 'aws')

        if region is not None:
            regions = [r for r in regions if r.name == region]
        if zone is not None:
            for r in regions:
                assert r.zones is not None, r
                r.set_zones([z for z in r.zones if z.name == zone])
            regions = [r for r in regions if r.zones]
        return regions

    @classmethod
    def zones_provision_loop(
        cls,
        *,
        region: str,
        num_nodes: int,
        instance_type: str,
        accelerators: Optional[Dict[str, int]] = None,
        use_spot: bool = False,
    ) -> Iterator[List[clouds.Zone]]:
        # TODO(suquark): Now we can return one zone at a time,
        # like other clouds,
        # because the new provisioner can failover to other zones pretty fast.
        # This will simplify our provision logic a lot.

        # AWS provisioner can handle batched requests, so yield all zones under
        # each region.
        regions = cls.regions_with_offering(instance_type,
                                            accelerators,
                                            use_spot,
                                            region=region,
                                            zone=None)
        for r in regions:
            assert r.zones is not None, r
            if num_nodes > 1:
                # When num_nodes > 1, we shouldn't pass a list of zones to the
                # AWS NodeProvider to try, because it may then place the nodes of
                # the same cluster in different zones. This is an artifact of the
                # current AWS NodeProvider implementation.
                for z in r.zones:
                    yield [z]
            else:
                yield r.zones

    @classmethod
    def _get_default_ami(cls, region_name: str, instance_type: str) -> str:
        komodo_ami = KOMODO_SKYPILOT_AMIS.get(region_name, None)
        if komodo_ami:
            logger.info(f"Using Komodo AMI for region {region_name}: {komodo_ami}")
            return komodo_ami

        acc = cls.get_accelerators_from_instance_type(instance_type)
        image_id = service_catalog.get_image_id_from_tag(
            'skypilot:gpu-ubuntu-2004', region_name, clouds='aws')
        if acc is not None:
            assert len(acc) == 1, acc
            acc_name = list(acc.keys())[0]
            if acc_name == 'K80':
                image_id = service_catalog.get_image_id_from_tag(
                    'skypilot:k80-ubuntu-2004', region_name, clouds='aws')
        if image_id is not None:
            return image_id
        # Raise ResourcesUnavailableError to make sure the failover in
        # CloudVMRayBackend will be correctly triggered.
        # TODO(zhwu): This is a information leakage to the cloud implementor,
        # we need to find a better way to handle this.
        raise exceptions.ResourcesUnavailableError(
            'No image found in catalog for region '
            f'{region_name}. Try setting a valid image_id.')

    @classmethod
    def _get_image_id(
        cls,
        image_id: Optional[Dict[Optional[str], str]],
        region_name: str,
        instance_type: str,
    ) -> str:
        if image_id is None:
            return cls._get_default_ami(region_name, instance_type)
        if None in image_id:
            image_id_str = image_id[None]
        else:
            assert region_name in image_id, image_id
            image_id_str = image_id[region_name]
        if image_id_str.startswith('skypilot:'):
            image_id_str = service_catalog.get_image_id_from_tag(image_id_str,
                                                                 region_name,
                                                                 clouds='aws')
            if image_id_str is None:
                # Raise ResourcesUnavailableError to make sure the failover
                # in CloudVMRayBackend will be correctly triggered.
                # TODO(zhwu): This is a information leakage to the cloud
                # implementor, we need to find a better way to handle this.
                raise exceptions.ResourcesUnavailableError(
                    f'No image found for region {region_name}')
        return image_id_str

    @classmethod
    def get_image_size(cls, image_id: str, region: Optional[str]) -> float:
        if image_id.startswith('skypilot:'):
            return DEFAULT_AMI_GB
        assert region is not None, (image_id, region)
        client = aws.client('ec2', region_name=region)
        image_not_found_message = (
            f'Image {image_id!r} not found in AWS region {region}.\n'
            f'\nTo find AWS AMI IDs: https://docs.aws.amazon.com/cli/latest/reference/ec2/describe-images.html#examples\n'  # pylint: disable=line-too-long
            'Example: ami-0729d913a335efca7')
        try:
            image_info = client.describe_images(ImageIds=[image_id])
            image_info = image_info.get('Images', [])
            if not image_info:
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(image_not_found_message)
            image_info = image_info[0]
            image_size = image_info['BlockDeviceMappings'][0]['Ebs'][
                'VolumeSize']
        except aws.botocore_exceptions().NoCredentialsError:
            # Fallback to default image size if no credentials are available.
            # The credentials issue will be caught when actually provisioning
            # the instance and appropriate errors will be raised there.
            return DEFAULT_AMI_GB
        except aws.botocore_exceptions().ClientError:
            with ux_utils.print_exception_no_traceback():
                raise ValueError(image_not_found_message) from None
        return image_size

    @classmethod
    def get_zone_shell_cmd(cls) -> Optional[str]:
        # The command for getting the current zone is from:
        # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-identity-documents.html  # pylint: disable=line-too-long
        command_str = (
            'curl -s http://169.254.169.254/latest/dynamic/instance-identity/document'  # pylint: disable=line-too-long
            f' | {constants.SKY_PYTHON_CMD} -u -c "import sys, json; '
            'print(json.load(sys.stdin)[\'availabilityZone\'])"')
        return command_str

    #### Normal methods ####

    def instance_type_to_hourly_cost(self,
                                     instance_type: str,
                                     use_spot: bool,
                                     region: Optional[str] = None,
                                     zone: Optional[str] = None) -> float:
        return service_catalog.get_hourly_cost(instance_type,
                                               use_spot=use_spot,
                                               region=region,
                                               zone=zone,
                                               clouds='aws')

    def accelerators_to_hourly_cost(self,
                                    accelerators: Dict[str, int],
                                    use_spot: bool,
                                    region: Optional[str] = None,
                                    zone: Optional[str] = None) -> float:
        del accelerators, use_spot, region, zone  # unused
        # AWS includes accelerators as part of the instance type.  Implementing
        # this is also necessary for e.g., the instance may have 4 GPUs, while
        # the task specifies to use 1 GPU.
        return 0

    def get_egress_cost(self, num_gigabytes: float):
        # In general, query this from the cloud:
        #   https://aws.amazon.com/s3/pricing/
        # NOTE: egress from US East (Ohio).
        # NOTE: Not accurate as the pricing tier is based on cumulative monthly
        # usage.
        if num_gigabytes > 150 * 1024:
            return 0.05 * num_gigabytes
        cost = 0.0
        if num_gigabytes >= 50 * 1024:
            cost += (num_gigabytes - 50 * 1024) * 0.07
            num_gigabytes -= 50 * 1024

        if num_gigabytes >= 10 * 1024:
            cost += (num_gigabytes - 10 * 1024) * 0.085
            num_gigabytes -= 10 * 1024

        if num_gigabytes > 1:
            cost += (num_gigabytes - 1) * 0.09

        cost += 0.0
        return cost

    @classmethod
    def get_default_instance_type(
            cls,
            cpus: Optional[str] = None,
            memory: Optional[str] = None,
            disk_tier: Optional[resources_utils.DiskTier] = None
    ) -> Optional[str]:
        return service_catalog.get_default_instance_type(cpus=cpus,
                                                         memory=memory,
                                                         disk_tier=disk_tier,
                                                         clouds='aws')

    # TODO: factor the following three methods, as they are the same logic
    # between Azure and AWS.
    @classmethod
    def get_accelerators_from_instance_type(
        cls,
        instance_type: str,
    ) -> Optional[Dict[str, int]]:
        return service_catalog.get_accelerators_from_instance_type(
            instance_type, clouds='aws')

    @classmethod
    def get_vcpus_mem_from_instance_type(
        cls,
        instance_type: str,
    ) -> Tuple[Optional[float], Optional[float]]:
        return service_catalog.get_vcpus_mem_from_instance_type(instance_type,
                                                                clouds='aws')

    def make_deploy_resources_variables(
            self,
            resources: 'resources_lib.Resources',
            cluster_name: resources_utils.ClusterName,
            region: 'clouds.Region',
            zones: Optional[List['clouds.Zone']],
            dryrun: bool = False) -> Dict[str, Any]:
        del dryrun  # unused
        assert zones is not None, (region, zones)

        region_name = region.name
        zone_names = [zone.name for zone in zones]

        r = resources
        # r.accelerators is cleared but .instance_type encodes the info.
        acc_dict = self.get_accelerators_from_instance_type(r.instance_type)
        if acc_dict is not None:
            custom_resources = json.dumps(acc_dict, separators=(',', ':'))
        else:
            custom_resources = None

        if r.extract_docker_image() is not None:
            image_id_to_use = None
        else:
            image_id_to_use = r.image_id
        image_id = self._get_image_id(image_id_to_use, region_name,
                                      r.instance_type)

        disk_encrypted = skypilot_config.get_nested(('aws', 'disk_encrypted'),
                                                    False)
        user_security_group_config = skypilot_config.get_nested(
            ('aws', 'security_group_name'), None)
        user_security_group = None
        if isinstance(user_security_group_config, str):
            user_security_group = user_security_group_config
        elif isinstance(user_security_group_config, list):
            for profile in user_security_group_config:
                if fnmatch.fnmatchcase(cluster_name.display_name,
                                       list(profile.keys())[0]):
                    user_security_group = list(profile.values())[0]
                    break
        security_group = user_security_group
        if security_group is None:
            security_group = DEFAULT_SECURITY_GROUP_NAME
            if resources.ports is not None:
                # Already checked in Resources._try_validate_ports
                security_group = USER_PORTS_SECURITY_GROUP_NAME.format(
                    cluster_name.display_name)
        elif resources.ports is not None:
            with ux_utils.print_exception_no_traceback():
                logger.warning(
                    f'Skip opening ports {resources.ports} for cluster {cluster_name!r}, '
                    'as `aws.security_group_name` in `~/.sky/config.yaml` is specified as '
                    f' {security_group!r}. Please make sure the specified security group '
                    'has requested ports setup; or, leave out `aws.security_group_name` '
                    'in `~/.sky/config.yaml`.')

        return {
            'instance_type': r.instance_type,
            'custom_resources': custom_resources,
            'disk_encrypted': disk_encrypted,
            'use_spot': r.use_spot,
            'region': region_name,
            'zones': ','.join(zone_names),
            'image_id': image_id,
            'security_group': security_group,
            'security_group_managed_by_skypilot':
                str(security_group != user_security_group).lower(),
            **AWS._get_disk_specs(r.disk_tier)
        }

    def _get_feasible_launchable_resources(
        self, resources: 'resources_lib.Resources'
    ) -> resources_utils.FeasibleResources:
        if resources.instance_type is not None:
            assert resources.is_launchable(), resources
            # Check the instance type is valid in the cloud
            regions = self.regions_with_offering(
                resources.instance_type,
                accelerators=resources.accelerators,
                use_spot=resources.use_spot,
                region=resources.region,
                zone=resources.zone)
            if not regions:
                # TODO: Add hints to all return values in this method to help
                #  users understand why the resources are not launchable.
                return resources_utils.FeasibleResources([], [], None)
            # Treat Resources(AWS, p3.2x, V100) as Resources(AWS, p3.2x).
            resources = resources.copy(accelerators=None)
            return resources_utils.FeasibleResources([resources], [], None)

        def _make(instance_list):
            resource_list = []
            for instance_type in instance_list:
                r = resources.copy(
                    cloud=AWS(),
                    instance_type=instance_type,
                    # Setting this to None as AWS doesn't separately bill /
                    # attach the accelerators.  Billed as part of the VM type.
                    accelerators=None,
                    cpus=None,
                    memory=None,
                )
                resource_list.append(r)
            return resource_list

        # Currently, handle a filter on accelerators only.
        accelerators = resources.accelerators
        if accelerators is None:
            # Return a default instance type with the given number of vCPUs.
            default_instance_type = AWS.get_default_instance_type(
                cpus=resources.cpus,
                memory=resources.memory,
                disk_tier=resources.disk_tier)
            if default_instance_type is None:
                return resources_utils.FeasibleResources([], [], None)
            else:
                return resources_utils.FeasibleResources(
                    _make([default_instance_type]), [], None)

        assert len(accelerators) == 1, resources
        acc, acc_count = list(accelerators.items())[0]
        (instance_list, fuzzy_candidate_list
        ) = service_catalog.get_instance_type_for_accelerator(
            acc,
            acc_count,
            use_spot=resources.use_spot,
            cpus=resources.cpus,
            memory=resources.memory,
            region=resources.region,
            zone=resources.zone,
            clouds='aws')
        if instance_list is None:
            return resources_utils.FeasibleResources([], fuzzy_candidate_list,
                                                     None)
        return resources_utils.FeasibleResources(_make(instance_list),
                                                 fuzzy_candidate_list, None)

    @classmethod
    @functools.lru_cache(maxsize=1)  # Cache since getting identity is slow.
    def check_credentials(cls) -> Tuple[bool, Optional[str]]:
        """Checks if the user has access credentials to this cloud."""

        dependency_installation_hints = (
            'AWS dependencies are not installed. '
            'Run the following commands:'
            f'\n{cls._INDENT_PREFIX}  $ pip install skypilot[aws]'
            f'\n{cls._INDENT_PREFIX}Credentials may also need to be set. '
            f'{cls._STATIC_CREDENTIAL_HELP_STR}')

        # Checks if the AWS CLI is installed properly
        proc = subprocess.run('aws --version',
                              shell=True,
                              check=False,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)
        if proc.returncode != 0:
            return False, dependency_installation_hints
        try:
            # Checks if aws boto is installed properly
            # pylint: disable=import-outside-toplevel, unused-import
            import boto3
            import botocore
        except ImportError:
            return False, dependency_installation_hints

        # Checks if AWS credentials 1) exist and 2) are valid.
        # https://stackoverflow.com/questions/53548737/verify-aws-credentials-with-boto3
        try:
            identity_str = cls.get_current_user_identity_str()
        except exceptions.CloudUserIdentityError as e:
            return False, str(e)

        static_credential_exists = os.path.isfile(
            os.path.expanduser('~/.aws/credentials'))
        hints = None
        identity_type = cls._current_identity_type()
        single_cloud_hint = (
            ' It will work if you use AWS only, but will cause problems '
            'if you want to use multiple clouds. To set up static credentials, '
            'try: aws configure')
        if identity_type == AWSIdentityType.SSO:
            hints = 'AWS SSO is set.'
            if static_credential_exists:
                hints += (
                    ' To ensure multiple clouds work correctly, please use SkyPilot '
                    'with static credentials (e.g., ~/.aws/credentials) by unsetting '
                    'the AWS_PROFILE environment variable.')
            else:
                hints += single_cloud_hint
        elif identity_type == AWSIdentityType.IAM_ROLE:
            # When using an IAM role, the credentials may not exist in the
            # ~/.aws/credentials file. So we don't check for the existence of the
            # file. This will happen when the user is on a VM (or
            # jobs-controller) created by an SSO account, i.e. the VM will be
            # assigned the IAM role: skypilot-v1.
            hints = f'AWS IAM role is set.{single_cloud_hint}'
        elif identity_type == AWSIdentityType.CONTAINER_ROLE:
            # Similar to the IAM ROLE, an ECS container may not store credentials
            # in the~/.aws/credentials file. So we don't check for the existence of
            # the file. i.e. the container will be assigned the IAM role of the
            # task: skypilot-v1.
            hints = f'AWS container-role is set.{single_cloud_hint}'
        else:
            # This file is required because it is required by the VMs launched on
            # other clouds to access private s3 buckets and resources like EC2.
            # `get_current_user_identity` does not guarantee this file exists.
            if not static_credential_exists:
                return (False, '~/.aws/credentials does not exist. ' +
                        cls._STATIC_CREDENTIAL_HELP_STR)

        # Fetch the AWS catalogs
        # pylint: disable=import-outside-toplevel
        from sky.clouds.service_catalog import aws_catalog

        # Trigger the fetch of the availability zones mapping.
        try:
            aws_catalog.get_default_instance_type()
        except RuntimeError as e:
            return False, (
                'Failed to fetch the availability zones for the account '
                f'{identity_str}. It is likely due to permission issues, please'
                ' check the minimal permission required for AWS: '
                'https://skypilot.readthedocs.io/en/latest/cloud-setup/cloud-permissions/aws.html'  # pylint: disable=
                f'\n{cls._INDENT_PREFIX}Details: '
                f'{common_utils.format_exception(e, use_bracket=True)}')
        return True, hints

    @classmethod
    def _current_identity_type(cls) -> Optional[AWSIdentityType]:
        proc = subprocess.run('aws configure list',
                              shell=True,
                              check=False,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)
        if proc.returncode != 0:
            return None
        stdout = proc.stdout.decode()

        # We determine the identity type by looking at the output of
        # `aws configure list`. The output looks like:
        #   Name                   Value         Type    Location
        #   ----                   -----         ----    --------
        #   profile                <not set>     None    None
        #   access_key     *       <not set>     sso     None
        #   secret_key     *       <not set>     sso     None
        #   region                 <not set>     None    None
        # We try to determine the identity type by looking for the
        # string "sso"/"iam-role" in the output, i.e. the "Type" column.

        def _is_access_key_of_type(type_str: str) -> bool:
            # The dot (.) does not match line separators.
            results = re.findall(fr'access_key.*{type_str}', stdout)
            if len(results) > 1:
                raise RuntimeError(
                    f'Unexpected `aws configure list` output:\n{stdout}')
            return len(results) == 1

        if _is_access_key_of_type(AWSIdentityType.SSO.value):
            return AWSIdentityType.SSO
        elif _is_access_key_of_type(AWSIdentityType.IAM_ROLE.value):
            return AWSIdentityType.IAM_ROLE
        elif _is_access_key_of_type(AWSIdentityType.CONTAINER_ROLE.value):
            return AWSIdentityType.CONTAINER_ROLE
        elif _is_access_key_of_type(AWSIdentityType.ENV.value):
            return AWSIdentityType.ENV
        elif _is_access_key_of_type(AWSIdentityType.ASSUME_ROLE_WITH_WEB_IDENTITY.value):
            return AWSIdentityType.ASSUME_ROLE_WITH_WEB_IDENTITY
        elif _is_access_key_of_type(AWSIdentityType.ASSUME_ROLE.value):
            return AWSIdentityType.ASSUME_ROLE
        else:
            return AWSIdentityType.SHARED_CREDENTIALS_FILE

    @classmethod
    def get_current_user_identity(cls) -> Optional[List[str]]:
        """Returns a [UserId, Account] list that uniquely identifies the user.

        These fields come from `aws sts get-caller-identity`. We permit the same
        actual user to:

          - switch between different root accounts (after which both elements
            of the list will be different) and have their clusters owned by
            each account be protected; or

          - within the same root account, switch between different IAM
            users, and treat [user_id=1234, account=A] and
            [user_id=4567, account=A] to be the *same*. Namely, switching
            between these IAM roles within the same root account will cause
            the first element of the returned list to differ, and will allow
            the same actual user to continue to interact with their clusters.
            Note: this is not 100% safe, since the IAM users can have very
            specific permissions, that disallow them to access the clusters
            but it is a reasonable compromise as that could be rare.

        Returns:
            A list of strings that uniquely identifies the user on this cloud.
            For identity check, we will fallback through the list of strings
            until we find a match, and print a warning if we fail for the
            first string.

        Raises:
            exceptions.CloudUserIdentityError: if the user identity cannot be
                retrieved.
        """
        try:
            sts = aws.client('sts')
            # The caller identity contains 3 fields: UserId, Account, Arn.
            # 1. 'UserId' is unique across all AWS entity, which looks like
            # "AROADBQP57FF2AEXAMPLE:role-session-name"
            # 2. 'Account' can be shared by multiple users under the same
            # organization
            # 3. 'Arn' is the full path to the user, which can be reused when
            # the user is deleted and recreated.
            # Refer to https://docs.aws.amazon.com/cli/latest/reference/sts/get-caller-identity.html # pylint: disable=line-too-long
            # and https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_variables.html#principaltable # pylint: disable=line-too-long
            user_info = sts.get_caller_identity()
            # Allow fallback to AccountId if UserId does not match, because:
            # 1. In the case where multiple IAM users belong a single root account,
            # those users normally share the visibility of the VMs, so we do not
            # need to identity them with each other. (There can be some cases,
            # when an IAM user is given a limited permission by the admin, we may
            # ignore that case for now, or print out a warning if the underlying
            # userid changed for a cluster).
            # 2. In the case where the multiple users belong to an organization,
            # those users will have different account id, so fallback works.
            user_ids = [user_info['UserId'], user_info['Account']]
        except aws.botocore_exceptions().NoCredentialsError as e:
            with ux_utils.print_exception_no_traceback():
                raise exceptions.CloudUserIdentityError(
                    'AWS credentials are not set. '
                    f'{cls._STATIC_CREDENTIAL_HELP_STR}\n'
                    f'{cls._INDENT_PREFIX}Details: `aws sts '
                    'get-caller-identity` failed with error:'
                    f' {common_utils.format_exception(e, use_bracket=True)}.'
                ) from None
        except aws.botocore_exceptions().ClientError as e:
            with ux_utils.print_exception_no_traceback():
                raise exceptions.CloudUserIdentityError(
                    'Failed to access AWS services with credentials. '
                    'Make sure that the access and secret keys are correct.'
                    f' {cls._STATIC_CREDENTIAL_HELP_STR}\n'
                    f'{cls._INDENT_PREFIX}Details: `aws sts '
                    'get-caller-identity` failed with error:'
                    f' {common_utils.format_exception(e, use_bracket=True)}.'
                ) from None
        except aws.botocore_exceptions().InvalidConfigError as e:
            # pylint: disable=import-outside-toplevel
            import awscli
            from packaging import version
            awscli_version = version.parse(awscli.__version__)
            if (awscli_version < version.parse('1.27.10') and
                    'configured to use SSO' in str(e)):
                with ux_utils.print_exception_no_traceback():
                    raise exceptions.CloudUserIdentityError(
                        'awscli is too old to use SSO. Run the following command to upgrade:'
                        f'\n{cls._INDENT_PREFIX}  $ pip install awscli>=1.27.10'
                        f'\n{cls._INDENT_PREFIX}You may need to log into SSO again after '
                        f'upgrading. {cls._sso_credentials_help_str()}'
                    ) from None
            with ux_utils.print_exception_no_traceback():
                raise exceptions.CloudUserIdentityError(
                    f'Invalid AWS configuration.\n'
                    f'  Reason: {common_utils.format_exception(e, use_bracket=True)}.'
                ) from None
        except aws.botocore_exceptions().TokenRetrievalError:
            # This is raised when the access token is expired, which mainly
            # happens when the user is using temporary credentials or SSO
            # login.
            with ux_utils.print_exception_no_traceback():
                raise exceptions.CloudUserIdentityError(
                    'AWS access token is expired.'
                    f' {cls._sso_credentials_help_str(expired=True)}') from None
        except Exception as e:  # pylint: disable=broad-except
            with ux_utils.print_exception_no_traceback():
                raise exceptions.CloudUserIdentityError(
                    f'Failed to get AWS user.\n'
                    f'  Reason: {common_utils.format_exception(e, use_bracket=True)}.'
                ) from None
        return user_ids

    @classmethod
    def get_current_user_identity_str(cls) -> Optional[str]:
        user_identity = cls.get_current_user_identity()
        if user_identity is None:
            return None
        identity_str = f'{user_identity[0]} [account={user_identity[1]}]'
        return identity_str

    def get_credential_file_mounts(self) -> Dict[str, str]:
        # The credentials file should not be uploaded if the user identity is
        # not SHARED_CREDENTIALS_FILE, since we cannot be sure if the currently
        # active user identity is the same as the one encoded in the credentials
        # file.  If they are indeed different identities, then uploading the
        # credential file to a launched node will make autostop/autodown/spot
        # controller misbehave.
        #
        # TODO(skypilot): ~/.aws/credentials is required for users using
        # multiple clouds.  If this file does not exist, users can launch on AWS
        # via AWS SSO or assumed IAM role (only when the user is on an AWS
        # cluster) and assign IAM role to the cluster.  However, if users launch
        # clusters in a non-AWS cloud, those clusters do not understand AWS IAM
        # role so will not be able to access private AWS EC2 resources and S3
        # buckets.
        #
        # TODO(zhwu/zongheng): We can also avoid uploading the credential file
        # for the cluster launched on AWS even if the user is using static
        # credentials. We need to define a mechanism to find out the cloud
        # provider of the cluster to be launched in this function and make sure
        # the cluster will not be used for launching clusters in other clouds,
        # e.g. jobs controller.
        if os.environ.get('__KOMODO_INSTALL_EXTERNAL_AWS_CREDENTIALS__', '') == '1':
            return {
                '/var/tmp/source_aws_credentials.sh': '/tmp/source_aws_credentials.sh',
                '~/.aws/credentials': '/tmp/aws_credentials',
            }
        if self._current_identity_type(
        ) != AWSIdentityType.SHARED_CREDENTIALS_FILE:
            return {}
        return {
            f'~/.aws/{filename}': f'~/.aws/{filename}'
            for filename in _CREDENTIAL_FILES
            if os.path.exists(os.path.expanduser(f'~/.aws/{filename}'))
        }

    def instance_type_exists(self, instance_type):
        return service_catalog.instance_type_exists(instance_type, clouds='aws')

    @classmethod
    def _get_disk_type(cls, disk_tier: resources_utils.DiskTier) -> str:
        return 'standard' if disk_tier == resources_utils.DiskTier.LOW else 'gp3'

    @classmethod
    def _get_disk_specs(
            cls,
            disk_tier: Optional[resources_utils.DiskTier]) -> Dict[str, Any]:
        tier = cls._translate_disk_tier(disk_tier)
        tier2iops = {
            resources_utils.DiskTier.HIGH: 7000,
            resources_utils.DiskTier.MEDIUM: 3500,
            resources_utils.DiskTier.LOW: 0,  # only gp3 is required to set iops
        }
        return {
            'disk_tier': cls._get_disk_type(tier),
            'disk_iops': tier2iops[tier],
            'disk_throughput': tier2iops[tier] // 16,
            'custom_disk_perf': tier != resources_utils.DiskTier.LOW,
        }

    @classmethod
    def check_quota_available(cls,
                              resources: 'resources_lib.Resources') -> bool:
        """Check if AWS quota is available based on `resources`.

        AWS-specific implementation of check_quota_available. The function
        works by matching the `instance_type` to the corresponding AWS quota
        code, and then using the boto3 Python API to query the `region` for
        the specific quota code (the `instance_type` and `region` as defined
        by `resources`).

        Returns:
            False if the quota is found to be zero, and True otherwise.
        Raises:
            ImportError: if the dependencies for AWS are not able to be installed.
            botocore.exceptions.ClientError: error in Boto3 client request.
        """

        instance_type = resources.instance_type
        region = resources.region
        use_spot = resources.use_spot

        # pylint: disable=import-outside-toplevel,unused-import
        from sky.clouds.service_catalog import aws_catalog

        quota_code = aws_catalog.get_quota_code(instance_type, use_spot)

        if quota_code is None:
            # Quota code not found in the catalog for the chosen instance_type, try provisioning anyway
            return True

        client = aws.client('service-quotas', region_name=region)
        try:
            response = client.get_service_quota(ServiceCode='ec2',
                                                QuotaCode=quota_code)
        except aws.botocore_exceptions().ClientError:
            # Botocore client connection not established, try provisioning anyways
            return True

        if response['Quota']['Value'] == 0:
            # Quota found to be zero, do not try provisioning
            return False

        # Quota found to be greater than zero, try provisioning
        return True

    @classmethod
    def query_status(cls, name: str, tag_filters: Dict[str, str],
                     region: Optional[str], zone: Optional[str],
                     **kwargs) -> List['status_lib.ClusterStatus']:
        # TODO(suquark): deprecate this method
        assert False, 'This code path should not be used.'

    @classmethod
    def create_image_from_cluster(cls,
                                  cluster_name: resources_utils.ClusterName,
                                  region: Optional[str],
                                  zone: Optional[str]) -> str:
        assert region is not None, (cluster_name.display_name,
                                    cluster_name.name_on_cloud, region)
        del zone  # unused

        image_name = f'skypilot-{cluster_name.display_name}-{int(time.time())}'

        status = provision_lib.query_instances('AWS',
                                               cluster_name.name_on_cloud,
                                               {'region': region})
        instance_ids = list(status.keys())
        if not instance_ids:
            with ux_utils.print_exception_no_traceback():
                raise RuntimeError(
                    f'Failed to find the source cluster {cluster_name.display_name!r} on '
                    'AWS.')

        if len(instance_ids) != 1:
            with ux_utils.print_exception_no_traceback():
                raise exceptions.NotSupportedError(
                    'Only support creating image from single '
                    f'instance, but got: {instance_ids}')

        instance_id = instance_ids[0]
        create_image_cmd = (
            f'aws ec2 create-image --region {region} --instance-id {instance_id} '
            f'--name {image_name} --output text')
        returncode, image_id, stderr = subprocess_utils.run_with_retries(
            create_image_cmd,
            retry_returncode=[255],
        )
        image_id = image_id.strip()
        subprocess_utils.handle_returncode(
            returncode,
            create_image_cmd,
            error_msg=
            f'Failed to create image from the source instance {instance_id}.',
            stderr=stderr,
            stream_logs=True)

        rich_utils.force_update_status(
            f'Waiting for the source image {cluster_name.display_name!r} from {region} to be available on AWS.'
        )
        # Wait for the image to be available
        wait_image_cmd = (
            f'aws ec2 wait image-available --region {region} --image-ids {image_id}'
        )
        returncode, _, stderr = subprocess_utils.run_with_retries(
            wait_image_cmd,
            retry_returncode=[255],
        )
        subprocess_utils.handle_returncode(
            returncode,
            wait_image_cmd,
            error_msg=
            f'The source image {image_id!r} creation fails to complete.',
            stderr=stderr,
            stream_logs=True)
        sky_logging.print(
            f'The source image {image_id!r} is created successfully.')
        return image_id

    @classmethod
    def maybe_move_image(cls, image_id: str, source_region: str,
                         target_region: str, source_zone: Optional[str],
                         target_zone: Optional[str]) -> str:
        del source_zone, target_zone  # unused
        if source_region == target_region:
            return image_id
        image_name = f'skypilot-cloned-from-{source_region}-{image_id}'
        copy_image_cmd = (f'aws ec2 copy-image --name {image_name} '
                          f'--source-image-id {image_id} '
                          f'--source-region {source_region} '
                          f'--region {target_region} --output text')
        returncode, target_image_id, stderr = subprocess_utils.run_with_retries(
            copy_image_cmd,
            retry_returncode=[255],
        )
        target_image_id = target_image_id.strip()
        subprocess_utils.handle_returncode(
            returncode,
            copy_image_cmd,
            error_msg=
            f'Failed to copy image {image_id!r} from {source_region} to {target_region}.',
            stderr=stderr,
            stream_logs=True)

        rich_utils.force_update_status(
            f'Waiting for the target image {target_image_id!r} on {target_region} to be '
            'available on AWS.')
        wait_image_cmd = (
            f'aws ec2 wait image-available --region {target_region} '
            f'--image-ids {target_image_id}')
        subprocess_utils.run_with_retries(
            wait_image_cmd,
            max_retry=5,
            retry_returncode=[255],
        )
        subprocess_utils.handle_returncode(
            returncode,
            wait_image_cmd,
            error_msg=
            f'The target image {target_image_id!r} creation fails to complete.',
            stderr=stderr,
            stream_logs=True)

        sky_logging.print(
            f'The target image {target_image_id!r} is created successfully.')

        rich_utils.force_update_status('Deleting the source image.')
        cls.delete_image(image_id, source_region)
        return target_image_id

    @classmethod
    def delete_image(cls, image_id: str, region: Optional[str]) -> None:
        assert region is not None, (image_id, region)
        delete_image_cmd = (f'aws ec2 deregister-image --region {region} '
                            f'--image-id {image_id}')
        returncode, _, stderr = subprocess_utils.run_with_retries(
            delete_image_cmd,
            retry_returncode=[255],
        )
        subprocess_utils.handle_returncode(
            returncode,
            delete_image_cmd,
            error_msg=f'Failed to delete image {image_id!r} on {region}.',
            stderr=stderr,
            stream_logs=True)

    @classmethod
    def is_label_valid(cls, label_key: str,
                       label_value: str) -> Tuple[bool, Optional[str]]:
        key_regex = re.compile(r'^(?!aws:)[\S]{1,127}$')
        value_regex = re.compile(r'^[\S]{0,255}$')
        key_valid = bool(key_regex.match(label_key))
        value_valid = bool(value_regex.match(label_value))
        error_msg = None
        if not key_valid:
            error_msg = (f'Invalid tag key {label_key} for AWS. '
                         'Key must start with any character except \'aws:\' '
                         'and must be 128 characters or fewer in length.')
        if not value_valid:
            error_msg = (f'Invalid tag value {label_value} for AWS. '
                         'Value must be 256 characters or fewer in length.')
        if not key_valid or not value_valid:
            return False, error_msg
        return True, None
