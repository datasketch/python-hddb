import pandas as pd
import os
from python_hddb.client import HdDB


os.environ["motherduck_token"] = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6ImRldkBkYXRhc2tldGNoLmNvIiwic2Vzc2lvbiI6ImRldi5kYXRhc2tldGNoLmNvIiwicGF0IjoiYldfUEROZm9ManJCeGRpUk9FRjlxT1JHb19ZR2pkSVJhRjJoT3E4cXJ3RSIsInVzZXJJZCI6IjdhMThkYjQ1LTAyNDktNGZmYy1hMDM3LTZkNmFmZTU4ZDMxZCIsImlzcyI6Im1kX3BhdCIsImlhdCI6MTcyNDY4NzY3OH0.fv5ppVsbS7L6MrrHhcjt-I-Ge9ipx-nyxz9faDqN__g"
# db = HdDB()
# db.drop_database("ddazal", "registros-usuarios")
# db.close()
# db = HdDB()
# df = pd.DataFrame({
#         'Name': ['John', 'Jane'],
#         'Age': [30, 25],
#         'City of Birth': ['New York', 'London'],
#         'rcd___id': [1, 2],
#         'SoT': [1, 2],
#         'SoT%': [3, 4]
#     })
# db.create_database([df], ["usuarios"])
# db.upload_to_motherduck("ddazal", "registros-usuarios")
# db.close()
db = HdDB()
fields = db.get_data_chunk("govtech-juventud-bizkaia", "poblacion-inactiva-euskadi", "output_1", {'start_row': 100, 'end_row':200})
print(fields)
db.close()