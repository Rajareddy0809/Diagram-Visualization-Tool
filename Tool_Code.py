import pandas as pd
import re
import os
import win32com.client
from collections import defaultdict

### --- STEP 1: Load and Clean Initial Dataset ---
file_path = r"C:\Users\gujjula.raja.reddy\OneDrive - Accenture\Desktop\Diagram Visualization Tool\Final_Sample.xlsx"
xls = pd.ExcelFile(file_path)
sheet_name = "バッチ一覧"

df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None)

# Identify the row that contains actual column headers dynamically
header_row_index = None
for i, row in df_raw.iterrows():
    if any("Target" in str(cell) for cell in row.values):
        header_row_index = i
        break

df = pd.read_excel(xls, sheet_name=sheet_name, header=header_row_index)

selected_columns = {
    "Target": "Target",
    "Input": "Input Id",
    "Unnamed: 18": "Input Name",
    "Output": "Output Id",
    "Unnamed: 20": "Output Name"
}

df_selected = df[list(selected_columns.keys())]

if df_selected.iloc[0].str.contains("テーブル/ビューID|テーブル名/ビュー名", na=False).any():
    df_selected = df_selected.iloc[1:].reset_index(drop=True)

df_selected.rename(columns=selected_columns, inplace=True)

df_selected = df_selected.map(lambda x: str(x).replace("_x000D_", "").replace("\r", "") if isinstance(x, str) else x)

### --- STEP 2: Process and Filter Data ---
columns_to_select = ["Target", "Input Id", "Input Name", "Output Id", "Output Name"]
df_filtered = df_selected[columns_to_select].dropna(subset=["Input Id"]).copy()

df_filtered.loc[:, "Input Id"] = df_filtered["Input Id"].astype(str).str.split("\n")
df_filtered.loc[:, "Input Name"] = df_filtered["Input Name"].astype(str).str.split("\n")

valid_rows = df_filtered["Input Id"].str.len() == df_filtered["Input Name"].str.len()
df_exploded = df_filtered[valid_rows].explode(["Input Id", "Input Name"])

df_cleaned = df_exploded[
    (df_exploded["Output Name"] != "e") & 
    (~df_exploded["Input Id"].str.startswith("TBMT", na=False)) & 
    (df_exploded["Input Id"].str.match(r".*\d{5}$", na=False))
]

### --- STEP 3: Remove Duplicate Input-Output Pairs ---
df_cleaned = df_cleaned.copy()
df_cleaned.loc[:, "Input-Output Pair"] = list(zip(df_cleaned["Input Id"], df_cleaned["Output Id"]))

df_final = df_cleaned.drop_duplicates(subset=["Input-Output Pair"], keep=False).drop(columns=["Input-Output Pair"])

### --- STEP 4: Extract Unique IDs and Remove WK Pattern ---
input_ids = df_final[['Input Id', 'Input Name']].rename(columns={'Input Id': 'Unique ID', 'Input Name': 'Unique Name'})
output_ids = df_final[['Output Id', 'Output Name']].rename(columns={'Output Id': 'Unique ID', 'Output Name': 'Unique Name'})

unique_ids = pd.concat([input_ids, output_ids], axis=0).drop_duplicates().reset_index(drop=True)

def is_wk_pattern(name):
    if pd.isna(name):  
        return False
    return bool(re.search(r'WK_\d{2}$|WK\d{2}$', name))

unique_ids_filtered = unique_ids[~unique_ids['Unique Name'].apply(is_wk_pattern)]

### --- STEP 5: Graph Processing (DFS to Remove Cycles and Self-loops) ---
valid_nodes_set = set(unique_ids_filtered['Unique ID'])

graph = defaultdict(list)
for _, row in df_final.iterrows():
    input_id, output_id = row['Input Id'], row['Output Id']
    graph[input_id].append(output_id)

removed_edges = set()
unique_paths = set()

def dfs(node, visited, path):
    if node in path:  
        cycle_index = path.index(node)
        removed_edges.add((path[cycle_index - 1], node))  
        return

    path.append(node)
    visited.add(node)

    for neighbor in graph.get(node, []):
        if neighbor == node:  
            removed_edges.add((node, node))  
            continue  
        dfs(neighbor, visited.copy(), path.copy())

    if len(path) > 1:
        unique_paths.add(tuple(path))  

for start_node in graph.keys():
    dfs(start_node, set(), [])

filtered_paths = set()
for path in unique_paths:
    filtered_path = tuple(node for node in path if node in valid_nodes_set)
    if len(filtered_path) > 1:  
        filtered_paths.add(filtered_path)

final_edges = set()
for path in filtered_paths:
    for i in range(len(path) - 1):
        final_edges.add((path[i], path[i + 1]))

filtered_removed_edges = set()
for edge in removed_edges:
    if edge[0] in valid_nodes_set and edge[1] in valid_nodes_set:
        final_edges.add(edge)  
        filtered_removed_edges.add(edge)  

all_nodes_in_edges = {node for edge in final_edges for node in edge if node is not None}
isolated_nodes = valid_nodes_set - all_nodes_in_edges  

for node in isolated_nodes:
    final_edges.add((node, None))  

### --- STEP 6: Save Processed Data ---
df_final_edges = pd.DataFrame(list(final_edges), columns=['Input Id', 'Output Id'])
id_to_name = dict(zip(unique_ids_filtered["Unique ID"], unique_ids_filtered["Unique Name"]))
df_final_edges["Input Name"] = df_final_edges["Input Id"].map(id_to_name)
df_final_edges["Output Name"] = df_final_edges["Output Id"].map(id_to_name)

# Define the output file path
input_directory = os.path.dirname(file_path)
output_file_path = os.path.join(input_directory, "Final_Unique_Edges.xlsx")
df_final_edges.to_excel(output_file_path, index=False)




import pandas as pd
import re
import os
def extract_valid_target_tables(file_path, sheet_name="バッチ一覧"):
    # ✅ Load the specific sheet
    df = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=1)

    # ✅ Rename first column to "Target" for clarity
    df.rename(columns={df.columns[0]: "Target"}, inplace=True)

    # ✅ Ensure required columns exist
    required_columns = ["Target", "Input", "Unnamed: 18"]  # "Input ID" and "Input Name"
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    # ✅ Filter rows where "Target" is NOT empty
    filtered_df = df[df["Target"].notna()].copy()

    # ✅ Prepare a list to store valid (ID, Name) pairs
    valid_targets = []

    # ✅ Iterate through rows to process "Input ID" and "Input Name"
    input_id_col = "Input"  # Column with Input IDs
    input_name_col = "Unnamed: 18"  # Column with Input Names

    for _, row in filtered_df.iterrows():
        # Split multiple IDs and names by newline or comma
        input_ids = re.split(r"[\n,]", str(row[input_id_col]).strip())
        input_names = re.split(r"[\n,]", str(row[input_name_col]).strip())

        # Ensure same number of IDs & Names after splitting
        if len(input_ids) != len(input_names):
            print(f"⚠️ Warning: Mismatched ID-Name count in row: {row}")
            continue  # Skip if mismatch occurs

        # ✅ Process each ID & Name
        for input_id, input_name in zip(input_ids, input_names):
            input_id = input_id.strip()  # Remove extra spaces
            input_name = input_name.strip()

            # ✅ Exclude IDs starting with "TBMT"
            if input_id.startswith("TBMT"):
                continue

            # ✅ Exclude Table Names ending with "WKXX" or "WK_XX"
            if re.search(r"WK\d{2}$|WK_\d{2}$", input_name):
                continue

            # ✅ Exclude IDs that are only "e"
            if input_id.lower() == "e":
                continue

            # ✅ Store valid (ID, Name) pairs
            valid_targets.append((input_id, input_name))

    # Convert to DataFrame for better readability
    valid_targets_df = pd.DataFrame(valid_targets, columns=["Input ID", "Input Name"])

    return valid_targets_df

# 🔹 Example Usage:
file_path = r"C:\Users\gujjula.raja.reddy\OneDrive - Accenture\Desktop\Diagram Visualization Tool\Final_Sample.xlsx"
filtered_target_tables = extract_valid_target_tables(file_path)
input_directory = os.path.dirname(file_path)
# Define the output file path
output_file_path = os.path.join(input_directory, "Final_target_input.xlsx")

# Save the filtered target tables to an Excel file
filtered_target_tables.to_excel(output_file_path, index=False)

# Provide the file for download


print("✅ Process completed. The filtered target tables are displayed.")


import pandas as pd
import win32com.client
class Graph2D:
    def __init__(self, spacing_constant=500, vertical_spacing=120):
        self.nodes = {}  
        self.edges = {}  
        self.visited = set()  
        self.spacing_constant = spacing_constant  
        self.vertical_spacing = vertical_spacing  
        self.n = 0  
        self.y_low = 0  
        self.sink_nodes = [] 
        self.sorted_sinks = []
        self.level_first_node = {}  
        self.first_x = 0  
        self.node_depths = {}  
        self.max_depth = 0  
        self.last_y_per_level = {} 

    def add_node(self, node_id):
        if node_id not in self.nodes:
            self.nodes[node_id] = None  
            self.edges[node_id] = []  

    def add_edge(self, from_node, to_node):
        if to_node in self.edges:
            self.edges[to_node].append(from_node)
        else:
            self.edges[to_node] = [from_node]

    def find_sink_nodes(self):
        all_nodes = set(self.nodes.keys())
        nodes_with_outgoing = {parent for children in self.edges.values() for parent in children}
        return list(all_nodes - nodes_with_outgoing)

    def calculate_depths(self):
        def dfs_longest_path(node):
            if node in self.node_depths:
                return self.node_depths[node]  
            max_depth = 0  
            for parent in self.edges.get(node, []):  
                max_depth = max(max_depth, dfs_longest_path(parent) + 1)
            self.node_depths[node] = max_depth  
            return max_depth

        self.sink_nodes = self.find_sink_nodes()
        self.node_depths = {}  
        self.max_depth = 0  

        longest_path_sink = None
        max_path_length = -1
        
        
        
        for sink in self.sink_nodes:
            path_length = dfs_longest_path(sink)
            if path_length > max_path_length:
                max_path_length = path_length
                longest_path_sink = sink

        self.max_depth = max_path_length
        return longest_path_sink  

    def place_nodes(self):
        longest_path_sink = self.calculate_depths()  
        self.n = len(self.nodes)  
        self.first_x = self.n + self.max_depth * self.spacing_constant  
        self.y_low = self.n  

        if longest_path_sink:
            x = self.first_x  
            y = self.y_low  
            self.last_y_per_level[longest_path_sink] = y
            self.dfs_place(longest_path_sink, x, y, self.node_depths[longest_path_sink], is_sink=True)
        self.sorted_sinks = sorted(self.sink_nodes, key=lambda node: self.node_depths.get(node, 0), reverse=True)
        for sink in self.sorted_sinks:
            if sink != longest_path_sink and sink not in self.visited:
                x = self.first_x  
                level = self.node_depths[longest_path_sink]
                y = self.y_low + self.vertical_spacing  
                self.last_y_per_level[level] = y  
                self.y_low = y
                self.dfs_place(sink, x, y, self.node_depths[longest_path_sink], is_sink=True)

    def dfs_place(self, node, child_x, child_y, level, is_sink=False):
     if node in self.visited:
        return  

     if is_sink:
        # 🔹 If it's a sink node, directly use the provided child_x and child_y
         x, y = child_x, child_y  
     else:
        # 🔹 Otherwise, calculate based on spacing
         x = child_x - self.spacing_constant  

        # ✅ If this is the first node in the level, use y_low
         if level not in self.last_y_per_level:
            y = self.y_low
         else:
            # ✅ If there's already a node in this level, check last Y value
            last_y = self.last_y_per_level[level]
            y = last_y + self.vertical_spacing if last_y >= self.y_low else self.y_low

        # ✅ Store this node's Y position for future reference  
     self.last_y_per_level[level] = y
     self.y_low = y 

    # 🚀 Directly place the node
     self.nodes[node] = (x, y)
     self.visited.add(node)

     next_level = level - 1
     for parent in self.edges.get(node, []):  
        if parent not in self.visited:
            self.dfs_place(parent, x, y, next_level, is_sink=False)

    def get_positions(self):
        return {node: pos for node, pos in self.nodes.items() if pos is not None}

# ✅ **Define File Names**
output_file = "Taregt_Table_Output.xlsx"
input_file = r"C:\Users\gujjula.raja.reddy\OneDrive - Accenture\Desktop\Diagram Visualization Tool\Final_Unique_Edges.xlsx"
target_file = r"C:\Users\gujjula.raja.reddy\OneDrive - Accenture\Desktop\Diagram Visualization Tool\Final_Target_Input.xlsx"

# ✅ **Read Input Data**
df = pd.read_excel(input_file)

# ✅ **User Input (Target Table)**
target_df = pd.read_excel(target_file)
if target_df.empty or "Input ID" not in target_df.columns:
    raise ValueError("❌ ERROR: No target tables found! Exiting the program.")
target_tables = target_df["Input ID"].unique().tolist()  # Get unique target table IDs

# ✅ **Step 1: Collect All Unique Edges**
merged_edges = set()  # Stores unique edges (parent, child)
unique_nodes = set()  # Stores all unique nodes
parent_map = {}
child_map = {}

# ✅ **Create Name Mapping from Input File**
id_to_name = {row["Input Id"]: row["Input Name"] for _, row in df.iterrows()}
id_to_name.update({row["Output Id"]: row["Output Name"] for _, row in df.iterrows()})

# ✅ **Build Parent-Child Relationship Mappings**
for _, row in df.iterrows():
    parent, child = row["Input Id"], row["Output Id"]
    parent_map.setdefault(child, []).append(parent)  # Store Parents
    child_map.setdefault(parent, []).append(child)  # Store Children

# ✅ **Traverse Each Target Table**
for target_table in target_tables:
    print(f"Processing Target Table: {target_table}...")

    # ✅ **Find Parents & Children**
    parents = parent_map.get(target_table, [])
    for parent in parents:
        merged_edges.add((parent, target_table))
        unique_nodes.update([parent, target_table])

    # ✅ **Find All Descendants**
    descendants = set()

    def traverse_children(node):
        if node in child_map:
            for child in child_map[node]:
                if child not in descendants:
                    descendants.add(child)
                    merged_edges.add((node, child))  # Store edge
                    unique_nodes.update([node, child])
                    traverse_children(child)

    # Start traversal from direct children
    if target_table in child_map:
        for direct_child in child_map[target_table]:
            descendants.add(direct_child)
            merged_edges.add((target_table, direct_child))
            unique_nodes.update([target_table, direct_child])
            traverse_children(direct_child)

# ✅ **Step 2: Build Merged Graph**
graph = Graph2D()

for parent, child in merged_edges:
    # ✅ Ensure child is not empty or NaN before adding nodes and edges
    if pd.notna(child) and child != "":
        graph.add_node(parent)
        graph.add_node(child)
        graph.add_edge(parent, child)
merged_edges_df = pd.DataFrame(list(merged_edges), columns=["Parent", "Child"])

# ✅ **Map Names to IDs for Output**
merged_edges_df["Parent Name"] = merged_edges_df["Parent"].map(id_to_name)
merged_edges_df["Child Name"] = merged_edges_df["Child"].map(id_to_name)

# ✅ **Save to Excel**


file_path = r"C:\Users\gujjula.raja.reddy\OneDrive - Accenture\Desktop\Diagram Visualization Tool\Final_Unique_Edges.xlsx"
input_directory = os.path.dirname(file_path)
# Define the output file path
output_file_path = os.path.join(input_directory, "Merged_Edges_1.xlsx")
merged_edges_df.to_excel(output_file_path, index=False)

# Save the filtered target tables to an Excel file


graph.place_nodes()
positions = graph.get_positions()

# ✅ **Create ERD in Excel**
excel = win32com.client.Dispatch("Excel.Application")
excel.Visible = True
wb = excel.Workbooks.Add()
ws = wb.Sheets(1)
ws.Name = "Merged_ERD"
shapes = ws.Shapes
table_shapes = {}

# ✅ **Position Nodes & Create Shapes**
for node, (x, y) in positions.items():
    display_name = id_to_name.get(node, node)  # ✅ Replace ID with Name if available
    shape = shapes.AddShape(13, x, y, 160, 60)
    shape.TextFrame.Characters().Text = display_name  
    shape.Fill.ForeColor.RGB = 0xEEDD99  
    shape.TextFrame.HorizontalAlignment = 3  
    shape.Line.ForeColor.RGB = 0x000000  
    shape.Line.Weight = 1 
    shape.TextFrame2.TextRange.Font.Fill.ForeColor.RGB = 0x000000 
    shape.TextFrame2.TextRange.ParagraphFormat.Alignment = 2  # Center align text horizontally
    shape.TextFrame2.VerticalAnchor = 3  # Center align text vertically
    table_shapes[node] = shape
 # Set text color to black


# 🚀 **Draw Directed Arrows with Mapped Names**
for child, parents in graph.edges.items():
    for parent in parents:
        if parent in table_shapes and child in table_shapes:
            shape1 = table_shapes[parent]
            shape2 = table_shapes[child]

            connector = shapes.AddConnector(2, 0, 0, 0, 0)  

            if shape1.Left > shape2.Left and shape1.Top > shape2.Top:  
    # shape1 is to the right and above shape2 (↘ diagonal)
              connector.ConnectorFormat.BeginConnect(shape1, 3)  
              connector.ConnectorFormat.EndConnect(shape2, 5)  

            elif shape1.Left > shape2.Left and shape1.Top < shape2.Top:  
    # shape1 is to the right and below shape2 (↗ diagonal)
              connector.ConnectorFormat.BeginConnect(shape1, 3)  
              connector.ConnectorFormat.EndConnect(shape2, 5)  

            elif shape1.Left < shape2.Left and shape1.Top > shape2.Top:  
    # shape1 is to the left and above shape2 (↙ diagonal)
             connector.ConnectorFormat.BeginConnect(shape1, 5)  
             connector.ConnectorFormat.EndConnect(shape2, 3)  

            elif shape1.Left < shape2.Left and shape1.Top < shape2.Top:  
    # shape1 is to the left and below shape2 (↖ diagonal)
             connector.ConnectorFormat.BeginConnect(shape1, 4)  
             connector.ConnectorFormat.EndConnect(shape2, 3)  

            elif shape1.Left == shape2.Left:  
    # shape1 and shape2 are vertically aligned → Top to Bottom
             connector.ConnectorFormat.BeginConnect(shape1, 3)  
             connector.ConnectorFormat.EndConnect(shape2, 3)  

            elif shape1.Top == shape2.Top:  
    # 🔹 Check if shape1 and shape2 are at **consecutive levels**
                 
                 connector.ConnectorFormat.BeginConnect(shape1, 5)  
                 connector.ConnectorFormat.EndConnect(shape2, 3)  
                


            else:  
    # Default fallback for any remaining cases
              connector.ConnectorFormat.BeginConnect(shape1, 1)  
              connector.ConnectorFormat.EndConnect(shape2, 3)  
  

            connector.Line.ForeColor.RGB = 0x000000  
            connector.Line.Weight = 2  
            connector.Line.EndArrowheadStyle = 3  

# 🚀 **Save and Close Excel**
# 🚀 **Highlight the Target Table Shape**
for target_table in target_tables:
    target_shape = table_shapes.get(target_table)
    if target_shape:
        target_shape.Fill.ForeColor.RGB = 0x0000FF  # Blue background
        target_shape.Line.ForeColor.RGB = 0x000000  # Black border
        target_shape.Line.Weight = 1  
        target_shape.TextFrame2.TextRange.Font.Fill.ForeColor.RGB = 0xFFFFFF  
        target_shape.TextFrame2.TextRange.Font.Bold = True  



    # 🚀 Highlight Sink Nodes in the Excel Diagram
for sink_node in graph.sink_nodes:
    sink_shape = table_shapes.get(sink_node)  # Get the shape for the sink node
    if sink_shape:
        sink_shape.Fill.ForeColor.RGB = 0xC1E1C1   # Set background color to Red
        sink_shape.Line.ForeColor.RGB = 0x000000  # Set border color to Black
        sink_shape.Line.Weight = 1  # Make the border thicker
        sink_shape.TextFrame2.TextRange.Font.Fill.ForeColor.RGB = 0x000000  # Change text color to White
        sink_shape.TextFrame2.TextRange.Font.Bold = True  # Make text bold


# 🚀 **Save and Close Excel**


# print(f"✅ Output saved as {output_file} with **Mapped Names in ERD!**")
# print(f"✅ Target table {target_table} has been highlighted in the ERD!")
ws.Application.ActiveWindow.DisplayGridlines = False  # Hide gridlines
ws.Application.ActiveWindow.DisplayHeadings = False   # Hide row & column headers
ws.Cells.Interior.ColorIndex = 2
wb.SaveAs(output_file)
excel.Quit()


# Save the filtered target tables to an Excel file

print(f"Output saved as {output_file} with **Mapped Names in ERD!**")











































