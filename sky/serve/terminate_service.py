import argparse
from sky.serve.serve_utils import terminate_services

def _terminate_service(service_name: str, purge: bool):
    terminate_services([service_name], purge)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Sky Serve Service')
    parser.add_argument('--service-name',
                        type=str,
                        help='Name of the service',
                        required=True)
    parser.add_argument('--purge',
                        action='store_true',
                        default=False)
    args = parser.parse_args()
    _terminate_service(args.service_name, args.purge)