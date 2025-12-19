pefrom lxml import etree
import pandas as pd

def generate_deep_impact_matrix(xml_path):
    tree = etree.parse(xml_path)
    root = tree.getroot()
    
    impact_data = []

    for folder in root.findall(".//FOLDER"):
        for mapping in folder.findall("MAPPING"):
            map_name = mapping.get('NAME')
            
            # Iterate through ALL transformations
            for trans in mapping.findall(".//TRANSFORMATION"):
                trans_name = trans.get('NAME')
                trans_type = trans.get('TYPE')

                # --- 1. EXPRESSIONS (The "IIF" Logic) ---
                if trans_type == 'Expression':
                    # Logic is stored in 'TRANSFORMFIELD' under 'EXPRESSION'
                    for field in trans.findall("TRANSFORMFIELD"):
                        expr = field.get('EXPRESSION')
                        port_name = field.get('NAME')
                        
                        # We only care if there is logic (ignore simple pass-throughs)
                        # We also check for quotes (') or numbers to spot likely hardcoding
                        if expr and (len(expr) > 0) and (expr != port_name):
                            impact_data.append({
                                "Mapping": map_name,
                                "Transformation": trans_name,
                                "Type": "Expression Logic",
                                "Target Column/Port": port_name,
                                "Code Snippet": expr
                            })

                # --- 2. FILTER TRANSFORMATION (The "WHERE" Clause) ---
                if trans_type == 'Filter':
                    for attr in trans.findall("TABLEATTRIBUTE"):
                        if attr.get('NAME') == 'Filter Condition':
                            impact_data.append({
                                "Mapping": map_name,
                                "Transformation": trans_name,
                                "Type": "Filter Condition",
                                "Target Column/Port": "FILTER",
                                "Code Snippet": attr.get('VALUE')
                            })

                # --- 3. ROUTER TRANSFORMATION (The Multi-Group Logic) ---
                if trans_type == 'Router':
                    # Router logic is inside GROUP tags, not attributes
                    for group in trans.findall("GROUP"):
                        group_expr = group.get('EXPRESSION')
                        group_name = group.get('NAME')
                        if group_expr:
                            impact_data.append({
                                "Mapping": map_name,
                                "Transformation": trans_name,
                                "Type": f"Router Group: {group_name}",
                                "Target Column/Port": "ROUTER",
                                "Code Snippet": group_expr
                            })

                # --- 4. SQL OVERRIDES (Retaining previous logic) ---
                if trans_type == 'Source Qualifier':
                    for attr in trans.findall("TABLEATTRIBUTE"):
                        if attr.get('NAME') == 'Sql Query' and attr.get('VALUE'):
                            impact_data.append({
                                "Mapping": map_name,
                                "Transformation": trans_name,
                                "Type": "SQL Override",
                                "Target Column/Port": "SOURCE",
                                "Code Snippet": attr.get('VALUE')
                            })

    df = pd.DataFrame(impact_data)
    return df

# Usage
# df = generate_deep_impact_matrix('Your_Workflow_Export.xml')
# df.to_excel("Informatica_Hardcoding_Report.xlsx", index=False)