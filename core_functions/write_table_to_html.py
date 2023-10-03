def write_table_to_html(df, player_list, filename, replace_in_cols = None):
    print(f"Writing to: {filename}")
    f = open(filename, "w")
    #    <meta http-equiv="refresh" content="1" >
    javascripts = """<script type="text/javascript" src="https://livejs.com/live.js"></script>"""
    table_style = """
<style>
    .styled-table {
        border-collapse: collapse;
        margin: 25px 0;
        font-size: 0.9em;
        font-family: sans-serif;
        min-width: 400px;
        border-radius: 5px 5px 0 0;
        overflow: hidden;
        box-shadow: 0 0 20px rgba(0, 0, 0, 0.15);
    }

    .styled-table thead tr {
        background-color: #009879;
        color: #ffffff;
        text-align: left;
    }

    .styled-table th,
    .styled-table td {
        padding: 12px 15px;
    }

    .styled-table tbody tr {
    border-bottom: 1px solid #dddddd;
    }

    .styled-table tbody tr:nth-of-type(even) {
        background-color: #f3f3f3;
    }

    .styled-table tbody tr:last-of-type {
        border-bottom: 2px solid #009879;
    }

    .styled-table tbody tr.active-row {
        font-weight: bold;
        color: #009879;
    }
</style>"""
    if replace_in_cols:
        columns = [column.replace("_score", "") for column in df.columns]
    else:
        columns = df.columns
    rows = [row for row in df.iter_rows()]
    all_rows_str = ""
    for num in range(0, len(rows)):
        row_num_str = "<tr>" + "".join([f"<td>{value}</td>" for value in rows[num]]) + "</tr>\n"
        all_rows_str = all_rows_str + row_num_str
    f.write(f"""{javascripts}
{table_style}

<div>
    <table class="styled-table">
        <thead>
            <tr>{"".join([(f"<th>{column}</th>") for column in columns])}</th></tr>
        </thead>
        <tbody>
{all_rows_str}
        </tbody>
    </table>
</div>

    """)
    f.close()
