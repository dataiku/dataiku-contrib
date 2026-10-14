# -*- coding: utf-8 -*-
"""Data Contract Generator macro.

Runs outside the Flow recipe model, so users do not need to create or select an
output dataset/folder. The JSON is always written to the project managed folder
named data_contracts.
"""
from __future__ import annotations

import html
import logging
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional

import dataiku
from dataiku.runnables import Runnable

from data_contract_generator.contract_builder import (
    build_contract,
    combine_description_parts,
    infer_decimal_places_from_rows,
    description_parts_from_mapping,
    filter_column_metadata_rows_for_schema,
    dumps_contract,
    merge_ordered_values,
    normalize_column_metadata_rows,
    normalize_list,
)

logging.basicConfig(level=logging.INFO, format="data-contract-generator %(levelname)s - %(message)s")
logger = logging.getLogger("data-contract-generator")

DEFAULT_FOLDER_NAME = "data_contracts"
CUSTOM_TAG_CHOICE_VALUE = "__custom__"

UNSUPPORTED_FOLDER_CONNECTION_TOKENS = (
    "openai",
    "azure openai",
    "anthropic",
    "cohere",
    "hugging",
    "llm",
    "large language model",
    "model provider",
    "api service",
)

STORAGE_FOLDER_CONNECTION_TOKENS = (
    "filesystem",
    "file system",
    "fs",
    "s3",
    "amazon",
    "aws",
    "azure",
    "adls",
    "abfs",
    "wasb",
    "blob",
    "gcs",
    "google cloud storage",
    "hdfs",
    "ftp",
    "sftp",
    "ssh",
    "webdav",
    "snowflake",
    "databricks",
    "unity",
    "volume",
    "warehouse",
    "customfs",
    "custom filesystem",
)


def _clean_string(value: Any, default: str = "") -> str:
    value = str(value or "").strip()
    return value if value else default


def _connection_name(value: Any, default: str = "") -> str:
    """Return a connection name from a DSS connection parameter value.

    Depending on DSS version/context, connection parameters can be provided as a
    string or as a small mapping. Keep this tolerant so the macro works across
    DSS 14.x installations.
    """
    if isinstance(value, Mapping):
        for key in ("name", "id", "connection", "value", "label"):
            candidate = _clean_string(value.get(key))
            if candidate:
                return candidate
        return default
    return _clean_string(value, default)


def _configured_folder_connection(plugin_config: Mapping[str, Any]) -> str:
    """Return the configured managed-folder storage connection name."""
    return _connection_name((plugin_config or {}).get("folder_connection_name"))


def _connection_text_from_payload(payload: Any) -> str:
    """Best-effort readable connection metadata for validation messages."""
    raw = _object_raw_payload(payload)
    parts: List[str] = []

    def collect(value: Any) -> None:
        if isinstance(value, Mapping):
            for key in (
                "name",
                "id",
                "label",
                "type",
                "connectionType",
                "connection_type",
                "connectionTypeId",
                "technology",
                "subtype",
            ):
                item = _clean_string(value.get(key))
                if item:
                    parts.append(item)
            for key in ("params", "details", "definition", "raw", "settings"):
                collect(value.get(key))

    collect(raw)
    return " ".join(parts).casefold()


def _validate_configured_folder_connection(connection_name: str) -> None:
    """Reject obvious API/LLM connections before asking DSS to create a folder.

    The native DSS CONNECTION selector may display non-storage connections, such
    as LLM/API connections. DSS will ultimately reject managed-folder creation
    on those connections; this validation turns that into a clearer error.
    """
    if not connection_name:
        return

    search_text = connection_name.casefold()
    try:
        client = dataiku.api_client()
        connection = client.get_connection(connection_name)
        search_text = "{} {}".format(search_text, _connection_text_from_payload(connection))
    except Exception:
        # If the API does not expose connection details in this context, let DSS
        # perform the final capability/permission check during folder creation.
        pass

    has_unsupported_hint = any(token in search_text for token in UNSUPPORTED_FOLDER_CONNECTION_TOKENS)
    has_storage_hint = any(token in search_text for token in STORAGE_FOLDER_CONNECTION_TOKENS)

    if has_unsupported_hint and not has_storage_hint:
        raise RuntimeError(
            "Connection {!r} appears to be an API or LLM connection, not a "
            "managed-folder storage connection. Choose a filesystem, S3, ADLS, "
            "GCS, HDFS, Snowflake, Databricks storage, or other connection that "
            "can create Dataiku managed folders.".format(connection_name)
        )


def _same_connection(left: Any, right: Any) -> bool:
    left_name = _clean_string(left).casefold()
    right_name = _clean_string(right).casefold()
    return bool(left_name and right_name and left_name == right_name)


def _config_bool(config: Dict[str, Any], key: str, default: bool = True) -> bool:
    value = (config or {}).get(key)
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() not in {"false", "0", "no", "n", "off"}
    return bool(value)


def _split_choice_string(value: str) -> List[str]:
    values: List[str] = []
    for line in str(value or "").splitlines():
        values.extend(part.strip() for part in line.split(","))
    return [value for value in values if value]


def _plugin_choice_values(plugin_config: Dict[str, Any], key: str, legacy_key: Optional[str] = None) -> List[str]:
    """Return configured plugin choice values as a de-duplicated ordered list."""
    raw_value = (plugin_config or {}).get(key)
    if (raw_value is None or raw_value == []) and legacy_key:
        raw_value = (plugin_config or {}).get(legacy_key)

    values: List[str] = []
    if raw_value is None:
        values = []
    elif isinstance(raw_value, str):
        values = _split_choice_string(raw_value)
    elif isinstance(raw_value, Mapping):
        values = normalize_list(raw_value)
    elif isinstance(raw_value, Iterable):
        for item in raw_value:
            if isinstance(item, str):
                values.extend(_split_choice_string(item))
            else:
                values.extend(normalize_list(item))
    else:
        values = normalize_list(raw_value)

    return merge_ordered_values(values)


def _row_column_name(row: Mapping[str, Any]) -> str:
    return _clean_string(
        row.get("column")
        or row.get("column_name")
        or row.get("name")
        or row.get("dataset_column")
    )


def _row_selected_tags(row: Mapping[str, Any]) -> List[str]:
    return [
        tag
        for tag in merge_ordered_values(
            row.get("tag")
            or row.get("tags")
            or row.get("sensitive_tag")
            or row.get("sensitive")
            or [],
        )
        if tag != CUSTOM_TAG_CHOICE_VALUE
    ]


def _row_custom_tags(row: Mapping[str, Any]) -> List[str]:
    if "use_custom_tags" in row and not _config_bool(row, "use_custom_tags", False):
        return []
    return merge_ordered_values(
        row.get("custom_tags")
        or row.get("custom_tag")
        or row.get("additional_tags")
        or [],
    )


def _validate_tag_configuration(column_metadata: Any, plugin_config: Dict[str, Any]) -> None:
    """Validate custom tag usage according to plugin settings."""
    if not _config_bool(plugin_config, "include_tag_field", True):
        return

    allow_custom_tags = _config_bool(plugin_config, "allow_custom_tag_values", False)
    allowed_tag_values = _plugin_choice_values(
        plugin_config,
        "tag_choices",
        legacy_key="sensitive_tag_choices",
    )
    allowed_lookup = {value.casefold() for value in allowed_tag_values}

    for row in normalize_column_metadata_rows(column_metadata):
        column_name = _row_column_name(row) or "<unknown column>"
        custom_tags = _row_custom_tags(row)
        if custom_tags and not allow_custom_tags:
            # DSS can retain hidden/stale macro parameter values from an earlier
            # run. When custom tags are disabled, ignore those stale values
            # instead of failing the macro.
            custom_tags = []

        if allow_custom_tags or not allowed_lookup:
            continue

        selected_tags = _row_selected_tags(row)
        invalid_tags = [tag for tag in selected_tags if tag.casefold() not in allowed_lookup]
        if invalid_tags:
            raise ValueError(
                "Column {} uses tag values that are not configured in plugin "
                "settings: {}".format(column_name, ", ".join(invalid_tags))
            )

def _safe_filename(dataset_name: str) -> str:
    base = re.sub(r"[^A-Za-z0-9_.-]+", "_", dataset_name).strip("._") or "dataset"
    filename = f"{base}_data_contract.json"
    return filename.replace("/", "_").replace("\\", "_").strip()


def _project_key(project: Any) -> Optional[str]:
    for attr in ("project_key", "key"):
        value = getattr(project, attr, None)
        if isinstance(value, str) and value.strip():
            return value.strip()
    try:
        summary = project.get_summary() or {}
        return summary.get("projectKey") or summary.get("project_key") or summary.get("key")
    except Exception:
        return None


def _folder_id_from_list_item(item: Dict[str, Any]) -> Optional[str]:
    for key in ("id", "odbId", "smartName"):
        value = item.get(key)
        if value:
            return str(value)
    return None


def _folder_name_from_list_item(item: Dict[str, Any]) -> Optional[str]:
    for key in ("name", "label", "id"):
        value = item.get(key)
        if value:
            return str(value)
    return None


def _folder_connection_from_mapping(value: Any) -> str:
    """Best-effort extraction of the connection used by a managed folder."""
    if not isinstance(value, Mapping):
        return ""

    direct_keys = (
        "connection",
        "connectionName",
        "connectionId",
        "connectionNameOrId",
        "connectionSmartName",
    )
    for key in direct_keys:
        candidate = _connection_name(value.get(key))
        if candidate:
            return candidate

    for key in ("params", "accessInfo", "settings", "raw", "definition"):
        candidate = _folder_connection_from_mapping(value.get(key))
        if candidate:
            return candidate

    return ""


def _folder_connection_name(project: Any, folder_id: str, folder_item: Optional[Mapping[str, Any]] = None) -> str:
    """Return the storage connection used by a managed folder when DSS exposes it."""
    candidate = _folder_connection_from_mapping(folder_item or {})
    if candidate:
        return candidate

    try:
        api_folder = project.get_managed_folder(folder_id)
    except Exception:
        api_folder = None

    if api_folder is not None:
        for method_name in ("get_settings", "get_definition", "get_config"):
            method = getattr(api_folder, method_name, None)
            if not callable(method):
                continue
            try:
                payload = method()
                raw_payload = _object_raw_payload(payload)
                candidate = _folder_connection_from_mapping(raw_payload)
                if candidate:
                    return candidate
                raw_params_method = getattr(payload, "get_raw_params", None)
                if callable(raw_params_method):
                    candidate = _folder_connection_from_mapping(raw_params_method())
                    if candidate:
                        return candidate
            except Exception:
                continue

    try:
        folder = _get_folder_handle(folder_id, project)
        info = folder.get_info()
        candidate = _folder_connection_from_mapping(info)
        if candidate:
            return candidate
    except Exception:
        pass

    return ""


def _validate_existing_folder_connection(
    project: Any,
    folder_id: str,
    folder_item: Mapping[str, Any],
    configured_connection: str,
) -> str:
    """Ensure an existing data_contracts folder matches the configured connection."""
    existing_connection = _folder_connection_name(project, folder_id, folder_item)
    if configured_connection and existing_connection and not _same_connection(existing_connection, configured_connection):
        raise RuntimeError(
            "A project managed folder named {!r} already exists, but it uses "
            "connection {!r} while the plugin is configured to use connection {!r}. "
            "Update the plugin setting to {!r}, or rename/remove the existing folder "
            "and rerun the macro so it can create the folder on the configured connection.".format(
                DEFAULT_FOLDER_NAME,
                existing_connection,
                configured_connection,
                existing_connection,
            )
        )
    return existing_connection


def _find_or_create_folder_id(project: Any, folder_name: str, connection_name: str) -> str:
    target_name = folder_name.strip()
    for folder_item in project.list_managed_folders():
        item_name = _folder_name_from_list_item(folder_item)
        if item_name == target_name:
            folder_id = _folder_id_from_list_item(folder_item)
            if folder_id:
                _validate_existing_folder_connection(
                    project=project,
                    folder_id=folder_id,
                    folder_item=folder_item,
                    configured_connection=connection_name,
                )
                return folder_id

    if not connection_name:
        raise RuntimeError(
            "The managed folder {!r} does not exist and no managed folder "
            "connection was configured in the plugin settings. Choose a "
            "managed-folder-capable connection, or create a project managed "
            "folder named {!r} before running the macro.".format(target_name, target_name)
        )

    _validate_configured_folder_connection(connection_name)

    try:
        created_folder = project.create_managed_folder(
            target_name,
            connection_name=connection_name,
        )
    except Exception as error:
        raise RuntimeError(
            "The managed folder {!r} does not exist and could not be created "
            "on connection {!r}. Choose a managed-folder-capable connection in "
            "the plugin settings where users are allowed to create managed "
            "folders, or ask an admin to create a project managed folder named "
            "{!r} before running the macro. Original error: {}".format(
                target_name, connection_name, target_name, error
            )
        )

    folder_id = getattr(created_folder, "id", None)
    if not folder_id:
        for folder_item in project.list_managed_folders():
            if _folder_name_from_list_item(folder_item) == target_name:
                folder_id = _folder_id_from_list_item(folder_item)
                break
    if not folder_id:
        raise RuntimeError(f"Created managed folder {target_name!r}, but could not resolve its id.")
    return str(folder_id)

def _get_folder_handle(folder_id: str, project: Any):
    project_key = _project_key(project)
    if project_key:
        return dataiku.Folder(folder_id, project_key=project_key)
    return dataiku.Folder(folder_id)


def _object_raw_payload(obj: Any) -> Any:
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj
    for method_name in ("get_raw", "get_definition", "get_config", "get_metadata"):
        method = getattr(obj, method_name, None)
        if not callable(method):
            continue
        try:
            return method()
        except Exception:
            continue
    return None


def _dataset_description(input_dataset: Any, project: Any, dataset_name: str) -> str:
    """Return the combined table short and long description, if present."""
    short_description = ""
    long_description = ""

    def merge_description_payload(payload: Any) -> None:
        nonlocal short_description, long_description
        payload_short, payload_long = description_parts_from_mapping(payload)
        if payload_short and not short_description:
            short_description = payload_short
        if payload_long and not long_description:
            long_description = payload_long

    for method_name in ("get_config", "get_metadata"):
        method = getattr(input_dataset, method_name, None)
        if not callable(method):
            continue
        try:
            merge_description_payload(method())
        except Exception:
            pass

    try:
        api_dataset = project.get_dataset(dataset_name)
    except Exception:
        api_dataset = None

    if api_dataset is not None:
        for method_name in ("get_settings", "get_definition", "get_config", "get_metadata"):
            method = getattr(api_dataset, method_name, None)
            if not callable(method):
                continue
            try:
                merge_description_payload(_object_raw_payload(method()))
            except Exception:
                continue

    return combine_description_parts(short_description, long_description)


class GenerateDataContract(Runnable):
    def __init__(self, project_key, config, plugin_config):
        self.project_key = project_key
        self.config = config or {}
        self.plugin_config = plugin_config or {}

    def get_progress_target(self):
        return (4, "NONE")

    def run(self, progress_callback):
        input_name = _clean_string(self.config.get("input_dataset"))
        if not input_name:
            raise ValueError("Select an input dataset before running this macro.")

        progress_callback(1)
        input_dataset = dataiku.Dataset(input_name, project_key=self.project_key)
        schema = input_dataset.read_schema()

        client = dataiku.api_client()
        project = client.get_project(self.project_key) if self.project_key else client.get_default_project()
        description = _dataset_description(input_dataset, project, input_name)

        progress_callback(2)
        try:
            inferred_decimal_places = infer_decimal_places_from_rows(input_dataset.iter_rows(), schema)
        except Exception as error:
            logger.warning("Unable to infer decimal precision from dataset values: %s", error)
            inferred_decimal_places = {}

        progress_callback(3)
        # Use the current macro parameter key first. This key intentionally differs
        # from older plugin versions so DSS does not prepopulate stale metadata
        # rows saved under the previous parameter name.
        raw_column_metadata = self.config.get("column_metadata_rows")
        if raw_column_metadata is None:
            raw_column_metadata = self.config.get("column_metadata")
        if raw_column_metadata is None:
            raw_column_metadata = self.config.get("sensitive_column_metadata")
        if raw_column_metadata is None:
            raw_column_metadata = []
        column_metadata, skipped_metadata_columns = filter_column_metadata_rows_for_schema(
            schema,
            raw_column_metadata,
        )
        if skipped_metadata_columns:
            logger.info(
                "Ignoring stale column metadata rows that do not exist in the selected dataset schema: %s",
                ", ".join(skipped_metadata_columns),
            )

        _validate_tag_configuration(column_metadata, self.plugin_config)

        contract = build_contract(
            schema=schema,
            description=description,
            column_metadata=column_metadata,
            include_schema_tags=False,
            inferred_decimal_places_by_column=inferred_decimal_places,
            include_tag_field=_config_bool(self.plugin_config, "include_tag_field", True),
            include_classification_field=_config_bool(self.plugin_config, "include_classification_field", True),
            include_category_field=_config_bool(self.plugin_config, "include_category_field", True),
        )

        json_text = dumps_contract(contract, indent=2)
        filename = _safe_filename(input_name)
        output_folder_id = _find_or_create_folder_id(
            project,
            DEFAULT_FOLDER_NAME,
            _configured_folder_connection(self.plugin_config),
        )
        folder = _get_folder_handle(output_folder_id, project)
        folder.upload_data(filename, json_text.encode("utf-8"))

        progress_callback(4)
        skipped_notice = ""
        if skipped_metadata_columns:
            skipped_notice = (
                "<p><strong>Note:</strong> Ignored stale metadata rows for columns "
                "not found in the selected dataset schema: {}.</p>"
            ).format(html.escape(", ".join(skipped_metadata_columns)))

        return (
            "<div>"
            "<h3>Data contract generated</h3>"
            f"<p><strong>Dataset:</strong> {html.escape(input_name)}</p>"
            f"<p><strong>Folder:</strong> {html.escape(DEFAULT_FOLDER_NAME)}</p>"
            f"<p><strong>File:</strong> {html.escape(filename)}</p>"
            f"{skipped_notice}"
            "</div>"
        )
