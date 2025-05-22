from markers import TEXT_END_MARKERS, TEXT_START_MARKERS, LEGALESE_END_MARKERS, LEGALESE_START_MARKERS, METADATA_MARKERS
# function mostly from https://github.com/c-w/Gutenberg/blob/master/gutenberg/cleanup/strip_headers.py

def extract_headers(paragraphs):
    out = []
    metadata = {}
    i = 0
    footer_found = False
    ignore_section = False

    for paragraph in paragraphs:
        reset = False

        if i <= 600:
            # Check if the header ends here
            if any(paragraph.startswith(token) for token in TEXT_START_MARKERS):
                reset = True

            for metadata_category, metadata_markers in METADATA_MARKERS.items():
                if metadata_category not in metadata:
                    reset = True
                    for marker in metadata_markers:
                        if paragraph.startswith(marker):
                            metadata[metadata_category] = paragraph.removeprefix(marker)
                            break
            # If it's the end of the header, delete the output produced so far.
            # May be done several times, if multiple lines occur indicating the
            # end of the header
            if reset:
                out = []
                continue

        if i >= 100:
            # Check if the footer begins here
            if any(paragraph.startswith(token) for token in TEXT_END_MARKERS):
                footer_found = True

            # If it's the beginning of the footer, stop output
            if footer_found:
                break

        if any(paragraph.startswith(token) for token in LEGALESE_START_MARKERS):
            ignore_section = True
            continue
        elif any(paragraph.startswith(token) for token in LEGALESE_END_MARKERS):
            ignore_section = False
            continue

        if not ignore_section:
            out.append(paragraph)
            i += 1
    return out, metadata
