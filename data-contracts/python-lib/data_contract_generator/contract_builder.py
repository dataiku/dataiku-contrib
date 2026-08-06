# -*- coding: utf-8 -*-
"""Build JSON data contracts from Dataiku dataset schemas.

This module has no Dataiku dependency so it can be unit-tested locally.
"""
from __future__ import annotations

import json
import math
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

MAX_INFERRED_DECIMAL_PLACES = 12
DEFAULT_DECIMAL_INFERENCE_ROW_LIMIT = 10000
CUSTOM_TAG_CHOICE_VALUE = "__custom__"
NONE_CHOICE_VALUE = "__none__"

INTEGER_TYPES = {
    "byte",
    "short",
    "tinyint",
    "smallint",
    "int",
    "integer",
    "bigint",
    "long",
}

NUMBER_TYPES = {
    "float",
    "double",
    "decimal",
    "numeric",
    "number",
    "real",
}

BOOLEAN_TYPES = {"boolean", "bool"}

DATE_TYPES = {"date"}

DATETIME_TYPES = {
    "datetime",
    "timestamp",
    "timestamp_ntz",
    "timestamp_ltz",
    "timestamp_tz",
    "timestamptz",
}

SHORT_DESCRIPTION_KEYS = (
    "shortDescription",
    "shortDesc",
    "short_description",
    "short_desc",
)

LONG_DESCRIPTION_KEYS = (
    "longDescription",
    "longDesc",
    "long_description",
    "long_desc",
    "description",
    "desc",
    "comment",
)

DESCRIPTION_NESTED_KEYS = (
    "metadata",
    "customMeta",
    "customFields",
    "params",
)


def _clean_description_text(value: Any) -> str:
    """Return a stripped description string, or an empty string."""
    if value is None:
        return ""
    text = str(value).strip()
    return text if text else ""


def _first_description_value(value: Mapping[str, Any], keys: Iterable[str]) -> str:
    """Return the first non-empty description value for the supplied keys."""
    for key in keys:
        text = _clean_description_text(value.get(key))
        if text:
            return text
    return ""


def description_parts_from_mapping(value: Any) -> Tuple[str, str]:
    """Return ``(short_description, long_description)`` from nested metadata."""
    if not isinstance(value, Mapping):
        return "", ""

    short_description = _first_description_value(value, SHORT_DESCRIPTION_KEYS)
    long_description = _first_description_value(value, LONG_DESCRIPTION_KEYS)

    for key in DESCRIPTION_NESTED_KEYS:
        nested_short, nested_long = description_parts_from_mapping(value.get(key))
        if nested_short and not short_description:
            short_description = nested_short
        if nested_long and not long_description:
            long_description = nested_long

    return short_description, long_description


def combine_description_parts(short_description: Any, long_description: Any) -> str:
    """Combine short and long descriptions for the contract description field."""
    values: List[str] = []
    seen = set()
    for value in (short_description, long_description):
        text = _clean_description_text(value)
        if not text:
            continue
        fingerprint = text.casefold()
        if fingerprint in seen:
            continue
        values.append(text)
        seen.add(fingerprint)
    return "\n\n".join(values)


def combined_description_from_mapping(value: Any) -> str:
    """Return combined short and long descriptions from one metadata mapping."""
    return combine_description_parts(*description_parts_from_mapping(value))


def _normalized_base_type(dataiku_type: Optional[str]) -> str:
    """Return a normalized Dataiku type token without precision suffixes."""
    if not dataiku_type:
        return ""
    raw_type = str(dataiku_type).strip().lower()
    return raw_type.split("(", 1)[0].strip()


def dataiku_type_to_json_type(dataiku_type: Optional[str]) -> str:
    """Convert a Dataiku storage/type name to a JSON-schema-style type."""
    base_type = _normalized_base_type(dataiku_type)
    if not base_type:
        return "string"

    if base_type in INTEGER_TYPES:
        return "integer"
    if base_type in NUMBER_TYPES:
        return "number"
    if base_type in BOOLEAN_TYPES:
        return "boolean"

    # JSON Schema represents dates and datetimes as strings with a format
    # annotation, which is added separately by dataiku_type_to_json_format().
    return "string"


def dataiku_type_to_json_format(dataiku_type: Optional[str]) -> Optional[str]:
    """Return a JSON Schema format annotation for date-like Dataiku types."""
    base_type = _normalized_base_type(dataiku_type)
    if not base_type:
        return None

    if base_type in DATE_TYPES:
        return "date"
    if base_type in DATETIME_TYPES or base_type.startswith("timestamp"):
        return "date-time"

    return None


def column_json_format(column: Mapping[str, Any]) -> Optional[str]:
    """Return the first date/date-time format exposed by column metadata."""
    for key in ("type", "storageType", "logicalType", "meaning", "semanticType"):
        value = column.get(key)
        if not value:
            continue
        json_format = dataiku_type_to_json_format(str(value))
        if json_format:
            return json_format
    return None


def column_description(column: Mapping[str, Any]) -> str:
    """Return a column description from schema metadata, or an empty string.

    Descriptions are never generated. The contract keeps the description key for
    every property, but the value is blank when the source dataset does not
    already provide a column description/comment. If Dataiku provides both short
    and long descriptions for a column, both are preserved in this value.
    """
    return combined_description_from_mapping(column)


def get_dataiku_column_type(column: Mapping[str, Any]) -> Optional[str]:
    """Return the first available Dataiku type value from a schema column."""
    for key in ("type", "storageType", "logicalType"):
        value = column.get(key)
        if value:
            return str(value)
    return None


def normalize_list(value: Any) -> List[str]:
    """Normalize Dataiku param values that should be string lists."""
    if value is None:
        return []
    if isinstance(value, str):
        value = value.strip()
        return [value] if value else []
    if isinstance(value, Mapping):
        for key in ("value", "label", "name"):
            candidate = str(value.get(key) or "").strip()
            if candidate:
                return [candidate]
        return []
    if isinstance(value, Iterable):
        return [str(v).strip() for v in value if str(v or "").strip()]
    return []




def _metadata_bool(value: Any, default: bool = False) -> bool:
    """Return a boolean from DSS metadata-row values."""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() not in {"", "false", "0", "no", "n", "off"}
    return bool(value)


def custom_tags_from_metadata_row(row: Mapping[str, Any]) -> List[str]:
    """Return custom tags only when the row's custom-tag input is enabled.

    New macro versions use ``use_custom_tags`` to open the separate custom tag
    input. If that key is present and false, ignore any stale hidden values that
    DSS may preserve from an earlier run. Older configs that do not have the key
    continue to preserve explicit custom tag values.
    """
    if "use_custom_tags" in row and not _metadata_bool(row.get("use_custom_tags"), False):
        return []
    return normalize_list(
        row.get("custom_tags")
        or row.get("custom_tag")
        or row.get("additional_tags")
        or []
    )


def drop_internal_tag_choices(values: Iterable[str]) -> List[str]:
    """Remove UI-only tag sentinels from generated contract values."""
    cleaned: List[str] = []
    for value in normalize_list(values):
        if value == CUSTOM_TAG_CHOICE_VALUE:
            continue
        cleaned.append(value)
    return cleaned

def merge_ordered_values(*value_lists: Iterable[str]) -> List[str]:
    """Merge string values while preserving order and removing duplicates."""
    merged: List[str] = []
    seen = set()
    for value_list in value_lists:
        for value in normalize_list(value_list):
            fingerprint = value.casefold()
            if fingerprint in seen:
                continue
            merged.append(value)
            seen.add(fingerprint)
    return merged


def build_column_lookup(schema: Sequence[Mapping[str, Any]]) -> Dict[str, str]:
    """Build a case-insensitive lookup of schema column names."""
    lookup: Dict[str, str] = {}
    for column in schema:
        name = str(column.get("name", "")).strip()
        if not name:
            continue
        lookup[name] = name
        lookup[name.lower()] = name
        lookup[name.upper()] = name
    return lookup


def resolve_column_name(column_name: str, lookup: Mapping[str, str]) -> Optional[str]:
    """Resolve an input column name against the schema lookup."""
    if column_name in lookup:
        return lookup[column_name]
    stripped = str(column_name or "").strip()
    if stripped in lookup:
        return lookup[stripped]
    if stripped.lower() in lookup:
        return lookup[stripped.lower()]
    if stripped.upper() in lookup:
        return lookup[stripped.upper()]
    return None


def get_column_tags(column: Mapping[str, Any]) -> List[str]:
    """Return tags from a Dataiku schema column, accepting a few known shapes."""
    candidates = [
        column.get("tags"),
        column.get("meaningTags"),
        (column.get("customFields") or {}).get("tags") if isinstance(column.get("customFields"), Mapping) else None,
    ]

    tags: List[str] = []
    for candidate in candidates:
        if not candidate:
            continue
        if isinstance(candidate, str):
            tags.append(candidate)
        elif isinstance(candidate, Iterable):
            tags.extend(str(v) for v in candidate if str(v or "").strip())
    return tags


def tag_values_from_schema_tags(column: Mapping[str, Any]) -> List[str]:
    """Read schema tags like tag, tag:<value>, tag=<value>, or legacy sensitive tags."""
    values: List[str] = []
    for tag in get_column_tags(column):
        tag_str = str(tag).strip()
        tag_lower = tag_str.lower()

        if tag_lower in {"tag", "sensitive"}:
            continue
        if tag_lower.startswith("tag:"):
            value = tag_str.split(":", 1)[1].strip()
        elif tag_lower.startswith("tag="):
            value = tag_str.split("=", 1)[1].strip()
        elif tag_lower.startswith("sensitive:"):
            value = tag_str.split(":", 1)[1].strip()
        elif tag_lower.startswith("sensitive="):
            value = tag_str.split("=", 1)[1].strip()
        else:
            continue
        if value:
            values.append(value)
    return normalize_list(values)


def _parse_json_or_lines(value: str) -> List[Mapping[str, Any]]:
    """Accept JSON or simple pipe/comma-delimited fallback text for local tests."""
    text = str(value or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except Exception:
        parsed = None

    if isinstance(parsed, list):
        return [row for row in parsed if isinstance(row, Mapping)]
    if isinstance(parsed, Mapping):
        return [parsed]

    rows: List[Mapping[str, Any]] = []
    for line in text.splitlines():
        parts = [part.strip() for part in line.replace("|", ",").split(",")]
        if len(parts) >= 1 and parts[0]:
            rows.append(
                {
                    "column": parts[0],
                    "tag": parts[1] if len(parts) > 1 else "",
                    "classification": parts[2] if len(parts) > 2 else "",
                    "category": parts[3] if len(parts) > 3 else "",
                }
            )
    return rows


def normalize_column_metadata_rows(value: Any) -> List[Mapping[str, Any]]:
    """Normalize the OBJECT_LIST config value for per-column metadata."""
    if value is None or value == "":
        return []
    if isinstance(value, str):
        return _parse_json_or_lines(value)
    if isinstance(value, Mapping):
        return [value]
    if isinstance(value, Iterable):
        return [row for row in value if isinstance(row, Mapping)]
    return []


def _first_string(*values: Any) -> str:
    """Return the first non-empty, non-sentinel string value."""
    for value in values:
        text = str(value or "").strip()
        if text and text != NONE_CHOICE_VALUE:
            return text
    return ""


def _metadata_row_column_name(row: Mapping[str, Any]) -> str:
    """Return the configured column name from a metadata row."""
    raw_column = (
        row.get("column")
        or row.get("column_name")
        or row.get("name")
        or row.get("dataset_column")
    )
    return str(raw_column or "").strip()


def filter_column_metadata_rows_for_schema(
    schema: Sequence[Mapping[str, Any]],
    column_metadata: Any,
) -> Tuple[List[Mapping[str, Any]], List[str]]:
    """Return metadata rows that apply to the selected schema and skipped columns.

    DSS can preserve OBJECT_LIST parameter values when a user opens the macro on
    another dataset. Those stale rows should not make contract generation fail.
    This helper resolves valid rows against the current schema and reports stale
    column names so the macro can ignore them safely.
    """
    lookup = build_column_lookup(schema)
    valid_rows: List[Mapping[str, Any]] = []
    skipped_columns: List[str] = []

    for row in normalize_column_metadata_rows(column_metadata):
        raw_column = _metadata_row_column_name(row)
        if not raw_column:
            continue

        resolved_column = resolve_column_name(raw_column, lookup)
        if not resolved_column:
            skipped_columns.append(raw_column)
            continue

        normalized_row = dict(row)
        normalized_row["column"] = resolved_column
        valid_rows.append(normalized_row)

    return valid_rows, skipped_columns


def column_metadata_by_column(
    schema: Sequence[Mapping[str, Any]],
    column_metadata: Any,
    include_schema_tags: bool = False,
) -> Dict[str, Dict[str, Any]]:
    """Return per-column optional metadata resolved against the schema."""
    metadata: Dict[str, Dict[str, Any]] = {}
    valid_rows, _skipped_columns = filter_column_metadata_rows_for_schema(schema, column_metadata)

    for row in valid_rows:
        resolved_column = _metadata_row_column_name(row)
        if not resolved_column:
            continue

        tag_values = drop_internal_tag_choices(
            merge_ordered_values(
                row.get("tag")
                or row.get("tags")
                or row.get("sensitive_tag")
                or row.get("sensitive")
                or [],
                custom_tags_from_metadata_row(row),
            )
        )
        classification = _first_string(row.get("classification"))
        category = _first_string(row.get("category"))

        metadata[resolved_column] = {
            "tag": tag_values,
            "classification": classification,
            "category": category,
        }

    if include_schema_tags:
        for column in schema:
            column_name = str(column.get("name", "")).strip()
            if not column_name or column_name in metadata:
                continue
            tag_values = tag_values_from_schema_tags(column)
            if tag_values:
                metadata[column_name] = {
                    "tag": tag_values,
                    "classification": "",
                    "category": "",
                }

    return metadata


# Backwards-compatible aliases for older tests/imports and configs.
normalize_sensitive_metadata_rows = normalize_column_metadata_rows
sensitive_metadata_by_column = column_metadata_by_column


def numeric_schema_column_names(schema: Sequence[Mapping[str, Any]]) -> List[str]:
    """Return schema columns that should be treated as JSON numbers."""
    names: List[str] = []
    for column in schema:
        column_name = str(column.get("name", "")).strip()
        if not column_name:
            continue
        if dataiku_type_to_json_type(get_dataiku_column_type(column)) == "number":
            names.append(column_name)
    return names


def decimal_places_from_value(value: Any, max_decimal_places: int = MAX_INFERRED_DECIMAL_PLACES) -> Optional[int]:
    """Return significant decimal places observed in one scalar value.

    Trailing zeroes are ignored. Float values are converted using a bounded
    significant-digit representation to reduce binary floating-point artifacts.
    """
    if value is None:
        return None

    if isinstance(value, bool):
        return None

    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        raw_text = format(value, ".12g")
    else:
        raw_text = str(value).strip()

    if raw_text == "":
        return None

    try:
        decimal_value = Decimal(raw_text)
    except (InvalidOperation, ValueError):
        return None

    if not decimal_value.is_finite():
        return None

    decimal_value = decimal_value.normalize()
    exponent = decimal_value.as_tuple().exponent
    places = max(0, -exponent)
    return min(places, max_decimal_places)


def infer_decimal_places_from_rows(
    rows: Iterable[Mapping[str, Any]],
    schema: Sequence[Mapping[str, Any]],
    max_decimal_places: int = MAX_INFERRED_DECIMAL_PLACES,
    max_rows: Optional[int] = DEFAULT_DECIMAL_INFERENCE_ROW_LIMIT,
) -> Dict[str, int]:
    """Infer max significant decimal places for numeric columns from sampled row data.

    The macro should not scan an entire large SQL table just to populate
    ``multipleOf``. By default, inference stops after the first 10,000 rows.
    Pass ``max_rows=None`` to scan all supplied rows, which is useful for small
    in-memory tests.
    """
    numeric_columns = numeric_schema_column_names(schema)
    if not numeric_columns:
        return {}

    inferred = {column_name: 0 for column_name in numeric_columns}
    rows_scanned = 0

    for row in rows:
        if max_rows is not None and rows_scanned >= max_rows:
            break

        rows_scanned += 1

        if not isinstance(row, Mapping):
            continue
        for column_name in numeric_columns:
            decimal_places = decimal_places_from_value(row.get(column_name), max_decimal_places=max_decimal_places)
            if decimal_places is None:
                continue
            if decimal_places > inferred[column_name]:
                inferred[column_name] = decimal_places

    return {column_name: places for column_name, places in inferred.items() if places > 0}


def multiple_of_from_decimal_places(decimal_places: int) -> Decimal:
    """Convert a decimal-place count to a JSON Schema multipleOf value."""
    decimal_places = int(decimal_places)
    if decimal_places <= 0:
        return Decimal("1")
    return Decimal("1").scaleb(-decimal_places)


def normalize_decimal_places_by_column(
    schema: Sequence[Mapping[str, Any]],
    decimal_places_by_column: Optional[Mapping[str, Any]],
) -> Dict[str, int]:
    """Resolve inferred decimal-place values against schema column names."""
    if not decimal_places_by_column:
        return {}

    lookup = build_column_lookup(schema)
    resolved: Dict[str, int] = {}

    for raw_column, raw_places in decimal_places_by_column.items():
        column_name = resolve_column_name(str(raw_column or ""), lookup)
        if not column_name:
            continue
        try:
            places = int(raw_places)
        except Exception:
            continue
        if places > 0:
            resolved[column_name] = min(places, MAX_INFERRED_DECIMAL_PLACES)

    return resolved


def build_contract(
    schema: Sequence[Mapping[str, Any]],
    description: str,
    column_metadata: Any = None,
    sensitive_column_metadata: Any = None,
    include_schema_tags: bool = False,
    include_schema_sensitive_tags: bool = False,
    inferred_decimal_places_by_column: Optional[Mapping[str, Any]] = None,
    include_tag_field: bool = True,
    include_classification_field: bool = True,
    include_category_field: bool = True,
) -> Dict[str, Any]:
    """Build the data contract object.

    The top-level `keys` and `version` sections are intentionally omitted.
    Descriptions are not generated: source descriptions are preserved when
    present and blank strings are emitted otherwise.
    """
    if not schema:
        raise ValueError("Input dataset schema is empty; cannot generate a data contract.")

    if column_metadata is None:
        column_metadata = sensitive_column_metadata

    per_column_metadata = column_metadata_by_column(
        schema=schema,
        column_metadata=column_metadata,
        include_schema_tags=include_schema_tags or include_schema_sensitive_tags,
    )
    decimal_places_lookup = normalize_decimal_places_by_column(schema, inferred_decimal_places_by_column)

    properties: Dict[str, Any] = {}

    for idx, column in enumerate(schema, start=1):
        column_name = str(column.get("name", "")).strip()
        if not column_name:
            raise ValueError(f"Schema column at position {idx} is missing a name")

        dataiku_type = get_dataiku_column_type(column)
        json_type = dataiku_type_to_json_type(dataiku_type)

        field_contract: Dict[str, Any] = {
            "order": idx,
            "type": json_type,
            "title": column_name,
            "description": column_description(column),
        }

        json_format = column_json_format(column)
        if json_format:
            field_contract["format"] = json_format

        if json_type == "number" and column_name in decimal_places_lookup:
            field_contract["multipleOf"] = multiple_of_from_decimal_places(decimal_places_lookup[column_name])

        optional_metadata = per_column_metadata.get(column_name)
        if optional_metadata is not None:
            tag_values = normalize_list(optional_metadata.get("tag"))
            classification = _first_string(optional_metadata.get("classification"))
            category = _first_string(optional_metadata.get("category"))

            if include_tag_field and tag_values:
                field_contract["tag"] = tag_values
            if include_classification_field and classification:
                field_contract["classification"] = classification
            if include_category_field and category:
                field_contract["category"] = category

        properties[column_name] = field_contract

    return {
        "type": "object",
        "properties": properties,
        "description": description,
    }


def _dumps_json_value(value: Any, indent: int, level: int) -> str:
    """Serialize JSON while keeping Decimal numbers as plain JSON numbers."""
    pad = " " * (indent * level)
    child_pad = " " * (indent * (level + 1))

    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, str):
        return json.dumps(value)
    if value is True:
        return "true"
    if value is False:
        return "false"
    if value is None:
        return "null"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return json.dumps(value)
    if isinstance(value, Mapping):
        if not value:
            return "{}"
        items = []
        for key, val in value.items():
            items.append(
                f"{child_pad}{json.dumps(str(key))}: {_dumps_json_value(val, indent, level + 1)}"
            )
        return "{\n" + ",\n".join(items) + "\n" + pad + "}"
    if isinstance(value, list):
        if not value:
            return "[]"
        items = [f"{child_pad}{_dumps_json_value(item, indent, level + 1)}" for item in value]
        return "[\n" + ",\n".join(items) + "\n" + pad + "]"

    # Fallback to a JSON string for unexpected scalar-like values.
    return json.dumps(str(value))


def dumps_contract(contract: Mapping[str, Any], indent: int = 2) -> str:
    """Serialize a contract as pretty JSON."""
    return _dumps_json_value(contract, indent=indent, level=0) + "\n"
