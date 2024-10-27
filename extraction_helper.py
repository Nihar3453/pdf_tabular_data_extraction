import pandas as pd


def compute_table_bbox(original_table_bbox, page_bbox):
    new_x = max(0.0, max(original_table_bbox[0], page_bbox[0]) - 2)
    new_top = max(0.0, max(original_table_bbox[1], page_bbox[1]) - 2)
    new_x1 = min(page_bbox[2], min(original_table_bbox[2], page_bbox[2]) + 2)
    new_bottom = min(page_bbox[3], min(original_table_bbox[3], page_bbox[3]) + 2)

    return tuple((new_x, new_top, new_x1, new_bottom))


def get_v_edges_without_boundary(vertical_edges):
    min_start_x = vertical_edges.get(["x0"]).min().get("x0")
    max_end_x = vertical_edges.get(["x1"]).max().get("x1")

    partial_vertical_edges = vertical_edges[(vertical_edges.x0 - 3) <= min_start_x]
    temp_vertical_edges = vertical_edges.drop(partial_vertical_edges.index)
    partial_vertical_edges = temp_vertical_edges[
        (temp_vertical_edges.x1 + 3) >= max_end_x
    ]
    temp_vertical_edges = temp_vertical_edges.drop(partial_vertical_edges.index)

    return temp_vertical_edges


def discard_incomplete_horizontal_edges(all_horizontal_edges):
    min_start_x = all_horizontal_edges.get(["x0"]).min().get("x0")
    max_end_x = all_horizontal_edges.get(["x1"]).max().get("x1")

    partial_horizontal_edges_min_x = all_horizontal_edges[
        (all_horizontal_edges.x0 - 3) > min_start_x
    ]
    all_horizontal_edges = all_horizontal_edges.drop(
        partial_horizontal_edges_min_x.index
    )
    partial_horizontal_edges_max_x = all_horizontal_edges[
        (all_horizontal_edges.x1 + 3) < max_end_x
    ]
    all_horizontal_edges = all_horizontal_edges.drop(
        partial_horizontal_edges_max_x.index
    )

    return all_horizontal_edges


def discard_incomplete_vertical_edges(
    all_vertical_edges, veritical_edges_no_vertical_boundary
):
    if veritical_edges_no_vertical_boundary.shape[0] > 0:
        min_start_y = veritical_edges_no_vertical_boundary.get(["y0"]).min().get("y0")

        partial_vertical_edges_min_y = veritical_edges_no_vertical_boundary[
            (veritical_edges_no_vertical_boundary.y0 - 3) > min_start_y
        ]
        all_vertical_edges = all_vertical_edges.drop(partial_vertical_edges_min_y.index)

    return all_vertical_edges


def find_actual_table_edges(h_edges_df, v_edges_df, v_edges_no_boundary):
    if v_edges_no_boundary.shape[0] > 0:
        max_end_y = v_edges_no_boundary.get(["y1"]).max().get("y1")
        min_end_y = v_edges_no_boundary.get(["y0"]).min().get("y0")

        partial_horizontal_edges = h_edges_df[(h_edges_df.y0 - 3) >= max_end_y]
        h_edges_df = h_edges_df.drop(partial_horizontal_edges.index)
        partial_horizontal_edges = h_edges_df[(h_edges_df.y0 + 3) <= min_end_y]
        h_edges_df = h_edges_df.drop(partial_horizontal_edges.index)

    return h_edges_df, v_edges_df


def add_missing_table_horizontal_boundary(h_edges_df, v_edges_no_boundary):
    if v_edges_no_boundary.shape[0] > 0 and h_edges_df.shape[0] > 0:
        max_end_y = v_edges_no_boundary.get(["y1"]).max().get("y1")
        min_start_x = h_edges_df.get(["x0"]).min().get("x0")
        max_end_x = h_edges_df.get(["x1"]).max().get("x1")

        partial_horizontal_edges = h_edges_df[(h_edges_df.y1 + 3) >= max_end_y]

        if partial_horizontal_edges.shape[0] == 0:
            new_edge_row = h_edges_df.iloc[0].copy()
            new_edge_row.at["top"] -= max_end_y - new_edge_row.at["y1"]
            new_edge_row.at["bottom"] -= max_end_y - new_edge_row.at["y1"]
            new_edge_row.at["doctop"] -= max_end_y - new_edge_row.at["y1"]
            new_edge_row.at["x0"] = min_start_x
            new_edge_row.at["x1"] = max_end_x
            new_edge_row.at["y1"] = max_end_y
            new_edge_row.at["y0"] = max_end_y
            h_edges_df = pd.concat(
                [h_edges_df, new_edge_row.to_frame().T], ignore_index=True
            )

    return h_edges_df


def get_table(page, table_settings):
    page_tables = page.find_tables(table_settings=table_settings)
    return page_tables


def filter_table_edges(table_bbox, original_edges_df):
    temp_edges_df = original_edges_df[original_edges_df.x0 >= table_bbox[0]]
    temp_edges_df = temp_edges_df[temp_edges_df.x0 <= table_bbox[2]]
    temp_edges_df = temp_edges_df[temp_edges_df.x1 >= table_bbox[0]]
    temp_edges_df = temp_edges_df[temp_edges_df.x1 <= table_bbox[2]]
    temp_edges_df = temp_edges_df[temp_edges_df.bottom >= table_bbox[1]]
    temp_edges_df = temp_edges_df[temp_edges_df.bottom <= table_bbox[3]]
    return temp_edges_df


def get_all_edges_of_page(tbl_find):
    all_edges_of_page = pd.DataFrame.from_dict(tbl_find.edges)
    if "pts" in all_edges_of_page.columns.values:
        all_edges_of_page = all_edges_of_page.drop(columns=["pts"])
    return all_edges_of_page


def get_words_in_cell(page, cell_bbox):
    new_page = page.crop(cell_bbox)
    extracted_info = new_page.extract_words(extra_attrs=["fontname", "size"])
    if len(extracted_info) > 0:
        return extracted_info[0]
    return {"fontname": "", "size": 0.0}


def get_first_cell_props(page, table_settings={}):
    tbl_find = page.debug_tablefinder(table_settings=table_settings)
    cell_bbox = tbl_find.cells[0]
    return get_words_in_cell(page, cell_bbox)


def get_last_cell_props(page, table_settings={}):
    tbl_find = page.debug_tablefinder(table_settings=table_settings)
    cell_bbox = tbl_find.cells[-1]
    return get_words_in_cell(page, cell_bbox)
