# -*- coding: utf-8 -*-
"""Dynamic dropdown population for the Data Contract Generator macro.

Column choices are read from the selected dataset schema. Tag, classification,
and category choices are read from plugin-level settings so organizations can
provide their own values without changing plugin code.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

import dataiku

TAG_SETTINGS_KEY = "tag_choices"
LEGACY_TAG_SETTINGS_KEY = "sensitive_tag_choices"
CLASSIFICATION_SETTINGS_KEY = "classification_choices"
CATEGORY_SETTINGS_KEY = "category_choices"

INCLUDE_TAG_FIELD_KEY = "include_tag_field"
INCLUDE_CLASSIFICATION_FIELD_KEY = "include_classification_field"
INCLUDE_CATEGORY_FIELD_KEY = "include_category_field"
ALLOW_CUSTOM_TAG_VALUES_KEY = "allow_custom_tag_values"
CUSTOM_TAG_CHOICE_VALUE = "__custom__"
CUSTOM_TAG_CHOICE_LABEL = "Other / custom tag"
NONE_CHOICE_VALUE = "__none__"
NONE_CHOICE_LABEL = "- None -"


MANAGED_FOLDER_CONNECTION_TYPE_TOKENS = (
    "filesystem",
    "file system",
    "fs",
    "hdfs",
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
    "ftp",
    "sftp",
    "ssh",
    "webdav",
    "customfs",
    "custom_fs",
    "custom filesystem",
    "managed folder",
    "managedfolder",
    "databricks",
    "unity",
    "volume",
    "snowflake",
    "snow",
    "warehouse",
)

NON_STORAGE_CONNECTION_TYPE_TOKENS = (
    "openai",
    "azure openai",
    "anthropic",
    "cohere",
    "hugging",
    "llm",
    "model",
    "api",
    "http",
    "jdbc",
    "sql",
    "postgres",
    "mysql",
    "oracle",
    "redshift",
    "bigquery",
    "elasticsearch",
    "elastic",
    "mongodb",
)


def _parameter_matches(parameter_name: Any, expected_name: str) -> bool:
    parameter_name = str(parameter_name or "")
    return parameter_name == expected_name or parameter_name.endswith("." + expected_name)


def _clean_string(value: Any) -> str:
    return str(value or "").strip()


def _split_config_string(value: str) -> List[str]:
    """Accept newline- or comma-separated strings for easier administration."""
    pieces: List[str] = []
    for line in str(value or "").splitlines():
        pieces.extend(part.strip() for part in line.split(","))
    return [piece for piece in pieces if piece]


def _normalize_choice_values(value: Any) -> List[str]:
    """Return configured choices as a de-duplicated ordered list."""
    raw_values: List[str] = []

    if value is None:
        raw_values = []
    elif isinstance(value, str):
        raw_values = _split_config_string(value)
    elif isinstance(value, dict):
        for key in ("value", "label", "name"):
            candidate = _clean_string(value.get(key))
            if candidate:
                raw_values = [candidate]
                break
    elif isinstance(value, Iterable):
        for item in value:
            if isinstance(item, dict):
                candidate = ""
                for key in ("value", "label", "name"):
                    candidate = _clean_string(item.get(key))
                    if candidate:
                        break
                if candidate:
                    raw_values.append(candidate)
            elif isinstance(item, str):
                raw_values.extend(_split_config_string(item))
            else:
                candidate = _clean_string(item)
                if candidate:
                    raw_values.append(candidate)
    else:
        candidate = _clean_string(value)
        if candidate:
            raw_values = [candidate]

    choices: List[str] = []
    seen = set()
    for raw_value in raw_values:
        choice = _clean_string(raw_value)
        if not choice:
            continue
        fingerprint = choice.casefold()
        if fingerprint in seen:
            continue
        choices.append(choice)
        seen.add(fingerprint)

    return choices


def _is_enabled(plugin_config: Dict[str, Any], setting_key: str) -> bool:
    value = (plugin_config or {}).get(setting_key)
    if value is None:
        return True
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() not in {"false", "0", "no", "n", "off"}
    return bool(value)


def _choices(values: Iterable[str]) -> Dict[str, List[Dict[str, str]]]:
    return {"choices": [{"value": value, "label": value} for value in values]}


def _settings_choices(
    plugin_config: Dict[str, Any],
    settings_key: str,
    enabled_key: str,
    fallback_settings_key: Optional[str] = None,
    include_none_choice: bool = False,
) -> Dict[str, List[Dict[str, str]]]:
    if not _is_enabled(plugin_config, enabled_key):
        return {"choices": []}

    values = _normalize_choice_values((plugin_config or {}).get(settings_key))
    if not values and fallback_settings_key:
        values = _normalize_choice_values((plugin_config or {}).get(fallback_settings_key))

    choices = []
    if include_none_choice:
        choices.append({"value": NONE_CHOICE_VALUE, "label": NONE_CHOICE_LABEL})
    choices.extend({"value": value, "label": value} for value in values)
    return {"choices": choices}




def _tag_settings_choices(plugin_config: Dict[str, Any]) -> Dict[str, List[Dict[str, str]]]:
    if not _is_enabled(plugin_config, INCLUDE_TAG_FIELD_KEY):
        return {"choices": []}

    values = _normalize_choice_values((plugin_config or {}).get(TAG_SETTINGS_KEY))
    if not values:
        values = _normalize_choice_values((plugin_config or {}).get(LEGACY_TAG_SETTINGS_KEY))

    choices = [{"value": value, "label": value} for value in values]

    # Custom tags are entered in the separate Custom tags field, not as a
    # pseudo-choice in the Tags multiselect.
    return {"choices": choices}

def _dataset_name_from_config(config: Dict[str, Any]) -> str:
    value = config.get("input_dataset")
    if isinstance(value, dict):
        for key in ("name", "smartName", "id"):
            candidate = _clean_string(value.get(key))
            if candidate:
                return candidate
        return ""
    return _clean_string(value)


def _project_key_from_payload(payload: Dict[str, Any], config: Dict[str, Any]) -> Optional[str]:
    for source in (payload, config):
        if not isinstance(source, dict):
            continue
        for key in ("projectKey", "project_key", "project"):
            value = _clean_string(source.get(key))
            if value:
                return value
    return None


def _column_choices(payload: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, List[Dict[str, str]]]:
    dataset_name = _dataset_name_from_config(config)
    if not dataset_name:
        return {"choices": []}

    project_key = _project_key_from_payload(payload, config)
    if project_key:
        dataset = dataiku.Dataset(dataset_name, project_key=project_key)
    else:
        dataset = dataiku.Dataset(dataset_name)

    schema = dataset.read_schema()
    column_names = sorted(
        _clean_string(column.get("name"))
        for column in schema
        if _clean_string(column.get("name"))
    )
    return _choices(column_names)



def _object_raw_payload(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    for method_name in ("get_raw", "get_definition", "get_settings", "get_config", "as_dict"):
        method = getattr(obj, method_name, None)
        if not callable(method):
            continue
        try:
            payload = method()
            if isinstance(payload, dict):
                return payload
            nested = _object_raw_payload(payload)
            if nested:
                return nested
        except Exception:
            continue
    raw = {}
    for attr in ("name", "type", "connectionType", "connection_type"):
        value = getattr(obj, attr, None)
        if value is not None:
            raw[attr] = value
    return raw


def _connection_name_from_payload(payload: Dict[str, Any]) -> str:
    for key in ("name", "id", "connection", "connectionName", "connectionNameOrId", "label"):
        candidate = _clean_string((payload or {}).get(key))
        if candidate:
            return candidate
    return ""


def _connection_type_from_payload(payload: Dict[str, Any]) -> str:
    text_parts = []
    for key in (
        "type",
        "connectionType",
        "connection_type",
        "connectionTypeId",
        "technology",
        "subtype",
        "label",
    ):
        value = _clean_string((payload or {}).get(key))
        if value:
            text_parts.append(value)

    for nested_key in ("params", "details", "definition", "raw", "settings"):
        nested = (payload or {}).get(nested_key)
        if isinstance(nested, dict):
            nested_type = _connection_type_from_payload(nested)
            if nested_type:
                text_parts.append(nested_type)

    return " ".join(text_parts)


def _connection_has_managed_folder_flag(payload: Dict[str, Any]) -> bool:
    for key in (
        "allowManagedFolders",
        "allow_managed_folders",
        "managedFolders",
        "useManagedFolders",
        "canCreateManagedFolders",
        "supportsManagedFolders",
    ):
        value = (payload or {}).get(key)
        if value is True:
            return True
        if isinstance(value, str) and value.strip().lower() in {"true", "1", "yes", "y", "on"}:
            return True

    for nested_key in ("params", "details", "definition", "raw", "settings"):
        nested = (payload or {}).get(nested_key)
        if isinstance(nested, dict) and _connection_has_managed_folder_flag(nested):
            return True

    return False


def _connection_type_can_support_managed_folders(payload: Dict[str, Any]) -> bool:
    type_text = _connection_type_from_payload(payload).casefold()
    name_text = _connection_name_from_payload(payload).casefold()
    search_text = "{} {}".format(type_text, name_text).strip()

    if _connection_has_managed_folder_flag(payload):
        return True

    if not search_text:
        return False

    # Exclude obvious API/LLM connections, but keep cloud/warehouse/storage
    # connections that may support managed folders once the DSS administrator
    # enables managed-folder usage on the connection.
    if any(token in search_text for token in NON_STORAGE_CONNECTION_TYPE_TOKENS):
        allowed_despite_generic_exclusion = (
            "databricks",
            "unity",
            "volume",
            "snowflake",
            "warehouse",
        )
        if not any(token in search_text for token in allowed_despite_generic_exclusion):
            return False

    if any(token in search_text for token in MANAGED_FOLDER_CONNECTION_TYPE_TOKENS):
        return True

    # DSS APIs do not always expose connection implementation details to plugin
    # parameter Python. When the user can see a connection but the type is opaque,
    # keep it selectable unless it was clearly filtered out above. Folder creation
    # will still be validated at runtime by DSS permissions/capabilities.
    return bool(name_text and not type_text)


def _connection_choice_label(name: str, payload: Dict[str, Any]) -> str:
    connection_type = _clean_string((payload or {}).get("type")) or _clean_string((payload or {}).get("connectionType"))
    if connection_type and connection_type.casefold() != name.casefold():
        return "{} ({})".format(name, connection_type)
    return name


def _managed_folder_connection_choices() -> Dict[str, List[Dict[str, str]]]:
    try:
        client = dataiku.api_client()
        listed = client.list_connections()
    except Exception:
        return {"choices": []}

    if isinstance(listed, dict):
        for key in ("connections", "items", "data"):
            if isinstance(listed.get(key), list):
                listed = listed[key]
                break

    if not isinstance(listed, list):
        listed = []

    choices = []
    seen = set()

    for item in listed:
        if isinstance(item, str):
            raw = {"name": item}
            name = _clean_string(item)
        else:
            raw = _object_raw_payload(item)
            name = _connection_name_from_payload(raw)

        if name:
            try:
                connection = client.get_connection(name)
                detail_raw = _object_raw_payload(connection)
                if detail_raw:
                    merged = dict(raw)
                    merged.update(detail_raw)
                    raw = merged
            except Exception:
                pass

        name = _connection_name_from_payload(raw) or name
        if not name:
            continue

        if not _connection_type_can_support_managed_folders(raw):
            continue

        fingerprint = name.casefold()
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        choices.append({"value": name, "label": _connection_choice_label(name, raw)})

    choices.sort(key=lambda choice: choice["label"].casefold())
    return {"choices": choices}

def do(payload, config, plugin_config, inputs):
    payload = payload or {}
    config = config or {}
    plugin_config = plugin_config or {}
    parameter_name = payload.get("parameterName")

    if _parameter_matches(parameter_name, "folder_connection_name"):
        return _managed_folder_connection_choices()

    if _parameter_matches(parameter_name, "column"):
        return _column_choices(payload, config)

    if _parameter_matches(parameter_name, "tag") or _parameter_matches(parameter_name, "sensitive_tag"):
        return _tag_settings_choices(plugin_config)

    if _parameter_matches(parameter_name, "classification"):
        return _settings_choices(
            plugin_config,
            CLASSIFICATION_SETTINGS_KEY,
            INCLUDE_CLASSIFICATION_FIELD_KEY,
            include_none_choice=True,
        )

    if _parameter_matches(parameter_name, "category"):
        return _settings_choices(
            plugin_config,
            CATEGORY_SETTINGS_KEY,
            INCLUDE_CATEGORY_FIELD_KEY,
            include_none_choice=True,
        )

    return {"choices": []}
