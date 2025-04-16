import pandas as pd
import os
import csv

class FlatFileHandler:
    def __init__(self, filename, delimiter=','):
        self.filename = filename
        self.delimiter = delimiter
        self.data = None
    
    def read_file(self):
        """Read data from flat file"""
        try:
            # Determine file type by extension
            ext = os.path.splitext(self.filename)[1].lower()
            
            if ext == '.csv':
                self.data = pd.read_csv(self.filename, delimiter=self.delimiter)
            elif ext == '.tsv':
                self.data = pd.read_csv(self.filename, delimiter='\t')
            elif ext == '.txt':
                self.data = pd.read_csv(self.filename, delimiter=self.delimiter)
            elif ext == '.xls' or ext == '.xlsx':
                self.data = pd.read_excel(self.filename)
            elif ext == '.json':
                self.data = pd.read_json(self.filename)
            else:
                return False, f"Unsupported file format: {ext}"
            
            return True, {"message": "File read successfully", "rows": len(self.data)}
        except Exception as e:
            return False, f"Failed to read file: {str(e)}"
    
    def get_schema(self):
        """Get schema (column names and types) of the flat file"""
        if self.data is None:
            success, result = self.read_file()
            if not success:
                return False, result
        
        try:
            columns = []
            for col in self.data.columns:
                dtype = str(self.data[col].dtype)
                columns.append({"name": col, "type": dtype})
            
            return True, columns
        except Exception as e:
            return False, f"Failed to get schema: {str(e)}"
    
    def preview_data(self, columns=None, limit=100):
        """Preview data from the flat file with selected columns"""
        if self.data is None:
            success, result = self.read_file()
            if not success:
                return False, result
        
        try:
            # If columns are specified, select only those
            df = self.data[columns] if columns else self.data
            
            # Limit rows
            df = df.head(limit)
            
            return True, {"data": df.to_dict('records'), "columns": df.columns.tolist()}
        except Exception as e:
            return False, f"Preview failed: {str(e)}"
    
    def export_data(self, columns=None):
        """Export data from flat file with selected columns"""
        if self.data is None:
            success, result = self.read_file()
            if not success:
                return False, result
        
        try:
            # If columns are specified, select only those
            df = self.data[columns] if columns else self.data
            
            return True, {"data": df, "count": len(df)}
        except Exception as e:
            return False, f"Export failed: {str(e)}"
    
    def save_to_file(self, data_df, output_filename, columns=None):
        """Save data to a new flat file"""
        try:
            # If columns specified, use only those columns
            if columns:
                data_df = data_df[columns]
            
            # Determine file type by extension and save accordingly
            ext = os.path.splitext(output_filename)[1].lower()
            
            if ext == '.csv':
                data_df.to_csv(output_filename, index=False)
            elif ext == '.tsv':
                data_df.to_csv(output_filename, sep='\t', index=False)
            elif ext == '.txt':
                data_df.to_csv(output_filename, sep=self.delimiter, index=False)
            elif ext == '.xls' or ext == '.xlsx':
                data_df.to_excel(output_filename, index=False)
            elif ext == '.json':
                data_df.to_json(output_filename, orient='records')
            else:
                return False, f"Unsupported output file format: {ext}"
            
            return True, {"count": len(data_df), "filename": output_filename}
        except Exception as e:
            return False, f"Failed to save file: {str(e)}"
