import argparse
import json

# pyright: reportMissingImports=false
import requests  # type: ignore[import-not-found]
# sample rest api in sample branch

def parse_key_value_pairs(items):
    result = {}
    for item in items or []:
        if "=" not in item:
            raise ValueError(f"Invalid argument format: '{item}'. Use key=value")
        key, value = item.split("=", 1)
        result[key] = value
    return result


def send_api_request(url, method="GET", params=None, headers=None, data=None, json_body=None, auth=None, timeout=30):
    response = requests.request(
        method=method.upper(),
        url=url,
        params=params,
        headers=headers,
        data=data,
        json=json_body,
        auth=auth,
        timeout=timeout,
    )

    try:
        response_data = response.json()
    except ValueError:
        response_data = response.text

    return {
        "status_code": response.status_code,
        "headers": dict(response.headers),
        "body": response_data,
    }


def main():
    parser = argparse.ArgumentParser(description="Send an HTTP request to an API from command-line arguments.")
    parser.add_argument("--url", required=True, help="API URL")
    parser.add_argument("--method", default="GET", help="HTTP method (GET, POST, PUT, DELETE, etc.)")
    parser.add_argument("--params", nargs="*", default=[], help="Request query parameters as key=value pairs")
    parser.add_argument("--headers", nargs="*", default=[], help="Headers as key=value pairs")
    parser.add_argument("--data", default=None, help="Request body as raw string")
    parser.add_argument("--json", default=None, help="JSON body string")
    parser.add_argument("--username", default=None, help="Basic auth username")
    parser.add_argument("--password", default=None, help="Basic auth password")
    parser.add_argument("--timeout", type=float, default=30, help="Request timeout in seconds")
    args = parser.parse_args()

    params = parse_key_value_pairs(args.params)
    headers = parse_key_value_pairs(args.headers)

    auth = None
    if args.username or args.password:
        auth = (args.username, args.password)

    json_body = None
    if args.json is not None:
        try:
            json_body = json.loads(args.json)
        except json.JSONDecodeError as exc:
            raise SystemExit(f"Invalid JSON in --json: {exc}")

    result = send_api_request(
        url=args.url,
        method=args.method,
        params=params,
        headers=headers,
        data=args.data,
        json_body=json_body,
        auth=auth,
        timeout=args.timeout,
    )

    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
