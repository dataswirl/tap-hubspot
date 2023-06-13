# tap-hubspot-sdk

`tap-hubspot-sdk` is a Singer tap for tap-hubspot-sdk.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Capabilities

* `catalog`
* `state`
* `discover`
* `about`
* `stream-maps`
* `schema-flattening`

## Configuration

### Accepted Config Options

| Setting             | Required | Default | Description |
|:--------------------|:--------:|:-------:|:------------|
| access_token        | True     | None    | The token to authenticate against the API service |
| api_version_1       | True    | v1.0    | The API version to request data from. |
| api_version_2       | True    | v2.0    | The API version to request data from. |
| api_version_3       | True    | v3.0    | The API version to request data from. |
| api_version_4       | True    | v4.0    | The API version to request data from. |
| start_date          | False    | None    | The earliest record date to sync |
| end_date            | False    | None    | The latest record date to sync |
| stream_maps         | False    | None    | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config   | False    | None    | User-defined config values to be used within map expressions. |
| flattening_enabled  | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth| False    | None    | The max depth to flatten schemas. |

### Meltano Variables

The following config values need to be set in order to use with Meltano. These can be set in `meltano.yml`, via
```meltano config tap-hubspot-sdk set --interactive```, or via the env var mappings shown above.

- `access_token:` access token from TAP_HUBSPOT_ACCESS_TOKEN variable
- `start_date:` start date
- `end_date:` end_date
- `api_version_1:` api version
- `api_version_2:` api version
- `api_version_3:` api version
- `api_version_4:` api version

A full list of supported settings and capabilities for this tap is available by running:

```bash
tap-hubspot-sdk --about
```

## Elastic License 2.0

The licensor grants you a non-exclusive, royalty-free, worldwide, non-sublicensable, non-transferable license to use, copy, distribute, make available, and prepare derivative works of the software.

## Installation

```bash
pipx install git+https://github.com/ryan-miranda-partners/tap-hubspot-sdk.git
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

A Hubspot access token is required to make API requests. (See [Hubspot API](https://developers.hubspot.com/docs/api/working-with-oauth) docs for more info)

## Usage

You can easily run `tap-hubspot-sdk` by itself or in a pipeline using [Meltano](https://meltano.com/).

## Stream Inheritance

This project uses parent-child streams. Learn more about them [here](https://gitlab.com/meltano/sdk/-/blob/main/docs/parent_streams.md).

### Executing the Tap Directly

```bash
tap-hubspot-sdk --version
tap-hubspot-sdk --help
tap-hubspot-sdk --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-hubspot-sdk` CLI interface directly using `poetry run`:

```bash
poetry run tap-hubspot-sdk --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-hubspot-sdk
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-hubspot-sdk --version
# OR run a test `elt` pipeline:
meltano elt tap-hubspot-sdk target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
