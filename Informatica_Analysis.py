import pandas as pd
from lxml import etree
import os

# --- USER CONFIGURATION ---
INPUT_XML_FILE = 'source_code.xml'       # The file you exported from Informatica
OUTPUT_EXCEL_FILE = 'Impact_Report.xlsx' # The file you want to create
# --------------------------

def parse_informatica_xml(xml_path):
    print(f"Reading XML file: {xml_path}...")
    
    try:
        # Check if file exists to avoid ugly errors
        if not os.path.exists(xml_path):
            print(f"ERROR: The file '{xml_path}' was not found in this folder.")
            return pd.DataFrame() # Return empty if fail

        # Parse the XML
        # Huge XMLs can be slow, but lxml handles them well
        tree = etree.parse(xml_path)
        root = tree.getroot()
    except Exception as e:
        print(f"Critical Error parsing XML: {e}")
        return pd.DataFrame()

    impact_data = []
    print("Scanning mappings for hardcoded logic...")

    # Namespaces are ignored here for simplicity; usually works for standard PowerCenter XMLs
    for folder in root.findall(".//FOLDER"):
        folder_name = folder.get('NAME')
        
        for mapping in folder.findall("MAPPING"):
            map_name = mapping.get('NAME')
            
            # Loop through all transformations in the mapping
            for trans in mapping.findall(".//TRANSFORMATION"):
                trans_name = trans.get('NAME')
                trans_type = trans.get('TYPE')

                # 1. EXPRESSIONS (Logic Formulas)
                if trans_type == 'Expression':
                    for field in trans.findall("TRANSFORMFIELD"):
                        expr = field.get('EXPRESSION')
                        port_name = field.get('NAME')
                        # Capture if expression is present and not just a pass-through
                        if expr and (len(expr) > 0) and (expr != port_name):
                            impact_data.append({
                                "Folder": folder_name,
                                "Mapping": map_name,
                                "Transformation": trans_name,
                                "Type": "Expression Logic",
                                "Target Column": port_name,
                                "Code Snippet": expr
                            })

                # 2. FILTER CONDITIONS (Hardcoded WHERE clauses)
                if trans_type == 'Filter':
                    for attr in trans.findall("TABLEATTRIBUTE"):
                        if attr.get('NAME') == 'Filter Condition':
                            impact_data.append({
                                "Folder": folder_name,
                                "Mapping": map_name,
                                "Transformation": trans_name,
                                "Type": "Filter Condition",
                                "Target Column": "FILTER_ROOT",
                                "Code Snippet": attr.get('VALUE')
                            })

                # 3. ROUTER GROUPS (Conditional Splitting)
                if trans_type == 'Router':
                    for group in trans.findall("GROUP"):
                        group_expr = group.get('EXPRESSION')
                        group_name = group.get('NAME')
                        if group_expr:
                            impact_data.append({
                                "Folder": folder_name,
                                "Mapping": map_name,
                                "Transformation": trans_name,
                                "Type": f"Router Group: {group_name}",
                                "Target Column": "ROUTER_GROUP",
                                "Code Snippet": group_expr
                            })

                # 4. SQL OVERRIDES (The "Black Box" Queries)
                # Handles Source Qualifiers (SQ)
                if trans_type == 'Source Qualifier':
                    for attr in trans.findall("TABLEATTRIBUTE"):
                        if attr.get('NAME') == 'Sql Query':
                            val = attr.get('VALUE')
                            if val:
                                impact_data.append({
                                    "Folder": folder_name,
                                    "Mapping": map_name,
                                    "Transformation": trans_name,
                                    "Type": "SQL Override (SQ)",
                                    "Target Column": "SOURCE_DB",
                                    "Code Snippet": val
                                })
                                
                # Handles Lookup Overrides
                if trans_type == 'Lookup Procedure':
                    for attr in trans.findall("TABLEATTRIBUTE"):
                        if attr.get('NAME') == 'Lookup Sql Override':
                            val = attr.get('VALUE')
                            if val:
                                impact_data.append({
                                    "Folder": folder_name,
                                    "Mapping": map_name,
                                    "Transformation": trans_name,
                                    "Type": "SQL Override (Lookup)",
                                    "Target Column": "LOOKUP_DB",
                                    "Code Snippet": val
                                })

    return pd.DataFrame(impact_data)

if __name__ == "__main__":
    # 1. Run the parser
    df_result = parse_informatica_xml(INPUT_XML_FILE)

    if not df_result.empty:
        # 2. Save to Excel
        print(f"Found {len(df_result)} logic points. Generating Excel...")
        try:
            df_result.to_excel(OUTPUT_EXCEL_FILE, index=False)
            print(f"SUCCESS! Audit report saved to: {OUTPUT_EXCEL_FILE}")
            print("You can now open the Excel file and Filter/Search for hardcoded values.")
        except Exception as e:
            print(f"Error writing to Excel: {e}")
            print("Make sure the Excel file is not currently open!")
    else:
        print("No data extracted. Check your XML file or the file name configuration.")
