"""SkyServeController: the central controller of SkyServe.

Responsible for autoscaling and replica management.
"""
import logging
import os
import threading
import time
import traceback
from typing import Any, Dict, List

import fastapi
import uvicorn

from sky import serve
from sky import sky_logging
from sky.serve import autoscalers
from sky.serve import replica_managers
from sky.serve import serve_state
from sky.serve import serve_utils
from sky.utils import common_utils
from sky.utils import ux_utils

logger = sky_logging.init_logger(__name__)


class SuppressSuccessGetAccessLogsFilter(logging.Filter):

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        return not ('GET' in message and '200' in message)


class SkyServeController:
    """SkyServeController: control everything about replica.

    This class is responsible for:
        - Starting and terminating the replica monitor and autoscaler.
        - Providing the HTTP Server API for SkyServe to communicate with.
    """

    def __init__(self, service_name: str, service_spec: serve.SkyServiceSpec,
                 task_yaml: str, host: str, port: int) -> None:
        self._service_name = service_name
        self._replica_manager: replica_managers.ReplicaManager = (
            replica_managers.SkyPilotReplicaManager(service_name=service_name,
                                                    spec=service_spec,
                                                    task_yaml_path=task_yaml))
        self._autoscaler: autoscalers.Autoscaler = (
            autoscalers.Autoscaler.from_spec(service_name, service_spec))
        self._host = host
        self._port = port
        self._app = fastapi.FastAPI()

    def _run_autoscaler(self):
        logger.info('Starting autoscaler.')
        while True:
            try:
                replica_infos = serve_state.get_replica_infos(
                    self._service_name)
                logger.info(f'All replica info: {replica_infos}')
                scaling_options = self._autoscaler.evaluate_scaling(
                    replica_infos)
                for scaling_option in scaling_options:
                    logger.info(f'Scaling option received: {scaling_option}')
                    if (scaling_option.operator ==
                            autoscalers.AutoscalerDecisionOperator.SCALE_UP):
                        assert (scaling_option.target is None or isinstance(
                            scaling_option.target, dict)), scaling_option
                        self._replica_manager.scale_up(scaling_option.target)
                    elif (scaling_option.operator ==
                          autoscalers.AutoscalerDecisionOperator.SCALE_DOWN):
                        assert isinstance(scaling_option.target,
                                          int), scaling_option
                        self._replica_manager.scale_down(scaling_option.target)
                    else:
                        with ux_utils.enable_traceback():
                            logger.error('Error in scaling_option.operator: '
                                         f'{scaling_option.operator}')
            except Exception as e:  # pylint: disable=broad-except
                # No matter what error happens, we should keep the
                # monitor running.
                logger.error('Error in autoscaler: '
                             f'{common_utils.format_exception(e)}')
                with ux_utils.enable_traceback():
                    logger.error(f'  Traceback: {traceback.format_exc()}')
            time.sleep(self._autoscaler.get_decision_interval())

    def run(self) -> None:

        @self._app.post('/controller/load_balancer_sync')
        async def load_balancer_sync(request: fastapi.Request):
            request_data = await request.json()
            # TODO(MaoZiming): Check aggregator type.
            request_aggregator: Dict[str, Any] = request_data.get(
                'request_aggregator', {})
            timestamps: List[int] = request_aggregator.get('timestamps', [])
            logger.info(f'Received {len(timestamps)} inflight requests.')
            self._autoscaler.collect_request_information(request_aggregator)
            return {
                'ready_replica_urls':
                    self._replica_manager.get_active_replica_urls()
            }

        @self._app.post('/controller/update_service')
        async def update_service(request: fastapi.Request):
            request_data = await request.json()
            try:
                version = request_data.get('version', None)
                if version is None:
                    return {'message': 'Error: version is not specified.'}
                update_mode_str = request_data.get(
                    'mode', serve_utils.DEFAULT_UPDATE_MODE.value)
                update_mode = serve_utils.UpdateMode(update_mode_str)
                logger.info(f'Update to new version {version} with '
                            f'update_mode {update_mode}.')
                # The yaml with the name latest_task_yaml will be synced
                # See sky/serve/core.py::update
                latest_task_yaml = serve_utils.generate_task_yaml_file_name(
                    self._service_name, version)
                service = serve.SkyServiceSpec.from_yaml(latest_task_yaml)
                logger.info(
                    f'Update to new version version {version}: {service}')

                self._replica_manager.update_version(version,
                                                     service,
                                                     update_mode=update_mode)
                new_autoscaler = autoscalers.Autoscaler.from_spec(
                    self._service_name, service)
                if not isinstance(self._autoscaler, type(new_autoscaler)):
                    logger.info('Autoscaler type changed to '
                                f'{type(new_autoscaler)}, updating autoscaler.')
                    old_autoscaler = self._autoscaler
                    self._autoscaler = new_autoscaler
                    self._autoscaler.load_dynamic_states(
                        old_autoscaler.dump_dynamic_states())
                self._autoscaler.update_version(version,
                                                service,
                                                update_mode=update_mode)
                return {'message': 'Success'}
            except Exception as e:  # pylint: disable=broad-except
                logger.error(f'Error in update_service: '
                             f'{common_utils.format_exception(e)}')
                return {'message': 'Error'}

        @self._app.on_event('startup')
        def configure_logger():
            uvicorn_access_logger = logging.getLogger('uvicorn.access')
            for handler in uvicorn_access_logger.handlers:
                handler.setFormatter(sky_logging.FORMATTER)

        threading.Thread(target=self._run_autoscaler).start()

        logger.info('SkyServe Controller started on '
                    f'http://0.0.0.0:{self._port}')

        uvicorn.run(self._app, host='0.0.0.0', port=self._port)


# TODO(tian): Probably we should support service that will stop the VM in
# specific time period.
def run_controller(service_name: str, service_spec: serve.SkyServiceSpec,
                   task_yaml: str, controller_port: int):
    # We expose the controller to the public network when running inside a
    # kubernetes cluster to allow external load balancers (example, for
    # high availability load balancers) to communicate with the controller.
    def _get_host():
        if 'KUBERNETES_SERVICE_HOST' in os.environ:
            return '0.0.0.0'
        else:
            return 'localhost'

    host = _get_host()
    controller = SkyServeController(service_name, service_spec, task_yaml, host,
                                    controller_port)
    controller.run()
