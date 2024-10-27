import pdfplumber
from tabulate import tabulate
import pandas as pd
from table_extraction.extraction_helper import (
    get_last_cell_props,
    get_first_cell_props,
    get_table,
    get_all_edges_of_page,
    filter_table_edges,
    discard_incomplete_horizontal_edges,
    discard_incomplete_vertical_edges,
    add_missing_table_horizontal_boundary,
    find_actual_table_edges,
    compute_table_bbox,
    get_v_edges_without_boundary,
)
from table_extraction.text_extraction_helpers import (
    compute_text_extraction_region,
    compute_next_text_extraction_region_start,
    extract_text_from_region,
    extract_text_from_page,
    split_long_words,
)


def add_page_text_to_final_result(final_result, new_text):
    if new_text is not None:
        final_result += new_text
    return final_result


def get_final_table_with_filled_na(table):
    tmp_table = pd.DataFrame.from_records(table)
    tmp_table = tmp_table.ffill(axis=1)
    tmp_table = tmp_table.map(lambda x: split_long_words(x, 10), na_action="ignore")
    tmp_table = tabulate(
        tmp_table.to_records(index=False), headers="firstrow", tablefmt="grid"
    )
    tmp_table += "\n\n"
    return tmp_table


def extract_all_tables(pdf_path):
    final_result = ""

    with pdfplumber.open(pdf_path) as pdf_plum:
        page_last_table_cell_props = None
        page_last_table = None
        remaining_text_from_last_page = None

        for i in range(len(pdf_plum.pages)):
            page = pdf_plum.pages[i]
            tbl_find = page.debug_tablefinder()
            all_edges_of_page = get_all_edges_of_page(tbl_find)
            initial_bbox = tuple(
                (page.bbox[0], page.bbox[1], page.bbox[2], page.bbox[1])
            )

            for j in range(len(tbl_find.tables)):
                table_bbox = tbl_find.tables[j].bbox
                new_table_bbox = compute_table_bbox(table_bbox, page.bbox)

                original_edges_df = filter_table_edges(
                    new_table_bbox, all_edges_of_page
                )

                vertical_edges = original_edges_df[original_edges_df.orientation == "v"]
                horizontal_edges = original_edges_df[
                    original_edges_df.orientation == "h"
                ]
                veritical_edges_no_vertical_boundary = get_v_edges_without_boundary(
                    vertical_edges
                )

                new_horizontal_edges = discard_incomplete_horizontal_edges(
                    horizontal_edges
                )
                new_vertical_edges = discard_incomplete_vertical_edges(
                    vertical_edges, veritical_edges_no_vertical_boundary
                )
                new_horizontal_edges = add_missing_table_horizontal_boundary(
                    new_horizontal_edges, veritical_edges_no_vertical_boundary
                )
                new_horizontal_edges, new_vertical_edges = find_actual_table_edges(
                    new_horizontal_edges,
                    new_vertical_edges,
                    veritical_edges_no_vertical_boundary,
                )

                new_tbl_settings = {}

                if new_horizontal_edges.shape[0] > 2:
                    new_tbl_settings["horizontal_strategy"] = "explicit"
                    new_tbl_settings[
                        "explicit_horizontal_lines"
                    ] = new_horizontal_edges.to_dict(orient="records")

                if new_vertical_edges.shape[0] > 2:
                    new_tbl_settings["vertical_strategy"] = "explicit"
                    new_tbl_settings[
                        "explicit_vertical_lines"
                    ] = new_vertical_edges.to_dict(orient="records")

                new_table = get_table(page, new_tbl_settings)

                remaining_text_before_table = None
                new_tbl_find = page.debug_tablefinder(table_settings=new_tbl_settings)

                if len(new_tbl_find.tables) > 0:
                    final_table_bbox = new_tbl_find.tables[0].bbox
                    final_table_bbox = compute_table_bbox(final_table_bbox, page.bbox)
                    extraction_region = compute_text_extraction_region(
                        initial_bbox, final_table_bbox
                    )
                    if extraction_region is not None:
                        remaining_text_before_table = extract_text_from_region(
                            extraction_region, page
                        )
                        initial_bbox = extraction_region

                    initial_bbox = compute_next_text_extraction_region_start(
                        initial_bbox, final_table_bbox
                    )

                else:
                    extraction_region_end = compute_next_text_extraction_region_start(
                        initial_bbox, new_table_bbox
                    )
                    extraction_region = compute_text_extraction_region(
                        initial_bbox, extraction_region_end
                    )
                    if extraction_region is not None:
                        remaining_text_before_table = extract_text_from_region(
                            extraction_region, page
                        )

                    initial_bbox = extraction_region_end

                if (
                    j == 0
                    and page_last_table_cell_props is not None
                    and page_last_table is not None
                ):
                    if len(new_table) == 0:
                        final_result += get_final_table_with_filled_na(page_last_table)
                        final_result = add_page_text_to_final_result(
                            final_result, remaining_text_from_last_page
                        )
                        remaining_text_from_last_page = None
                        final_result = add_page_text_to_final_result(
                            final_result, remaining_text_before_table
                        )
                        remaining_text_before_table = None

                    else:
                        page_first_cell_props = get_first_cell_props(
                            page, new_tbl_settings
                        )

                        if page_first_cell_props[
                            "fontname"
                        ] == page_last_table_cell_props["fontname"] and round(
                            page_first_cell_props["size"], 2
                        ) == round(
                            page_last_table_cell_props["size"], 2
                        ):
                            first_table = new_table[0].extract()
                            page_last_table.extend(first_table)
                            final_result += get_final_table_with_filled_na(
                                page_last_table
                            )
                        else:
                            final_result += get_final_table_with_filled_na(
                                page_last_table
                            )
                            final_result = add_page_text_to_final_result(
                                final_result, remaining_text_from_last_page
                            )
                            remaining_text_from_last_page = None
                            final_result = add_page_text_to_final_result(
                                final_result, remaining_text_before_table
                            )
                            remaining_text_before_table = None
                            final_result += get_final_table_with_filled_na(
                                new_table[0].extract()
                            )

                    page_last_table_cell_props = None
                    page_last_table = None

                elif len(new_table) > 0:
                    if j < (len(tbl_find.tables) - 1) or i == (len(pdf_plum.pages) - 1):
                        if j == 0:
                            final_result = add_page_text_to_final_result(
                                final_result, remaining_text_before_table
                            )
                            remaining_text_before_table = None
                        final_result += get_final_table_with_filled_na(
                            new_table[0].extract()
                        )
                    else:
                        final_result = add_page_text_to_final_result(
                            final_result, remaining_text_before_table
                        )
                        remaining_text_before_table = None
                        page_last_table_cell_props = get_last_cell_props(
                            page, new_tbl_settings
                        )
                        page_last_table = new_table[0].extract()

                elif len(new_table) == 0:
                    final_result = add_page_text_to_final_result(
                        final_result, remaining_text_before_table
                    )
                    remaining_text_before_table = None

                all_edges_of_page.drop(original_edges_df.index)

            if page.bbox[3] > initial_bbox[1]:
                initial_bbox = tuple(
                    (
                        initial_bbox[0],
                        initial_bbox[1],
                        initial_bbox[2],
                        page.bbox[3],
                    )
                )
                remaining_page_after_table = page.within_bbox(initial_bbox)
                remaining_text_from_last_page = extract_text_from_page(
                    remaining_page_after_table
                )

            if len(tbl_find.tables) == 0:
                if page_last_table is not None:
                    final_result += get_final_table_with_filled_na(page_last_table)
                    page_last_table = None
                    page_last_table_cell_props = None

                final_result = add_page_text_to_final_result(
                    final_result, remaining_text_from_last_page
                )
                remaining_text_from_last_page = None

    return final_result
