import numpy as np
import pandas as pd
import tabula
import os


def pdf_to_csv(pdf_path, password, file_name, upload_folder_path):
    # readingthepdfbody
    dfs = tabula.read_pdf(pdf_path, password=password, pages="all", lattice=True, multiple_tables=True,
                          pandas_options={'header': None}, )

    # readingthepdfforheaderdetails
    dfs1 = tabula.read_pdf(pdf_path, password=password, stream=True, pages=1, area=[0, 0, 50, 100], relative_area=True,
                           pandas_options={'header': None}, )

    # Extracttheheaderdetailsinaloop
    finaldf1 = []
    for df in dfs1[0:]:
        finaldf1.append(df)

    finaldf1 = pd.concat(finaldf1)

    # Extractstatementdetails
    Namelist = finaldf1.iloc[2, 0].split(' ', 3)[2:]
    Name = ' '.join(str(e) for e in Namelist)
    Mobile = finaldf1.iloc[3, 0].split(" ")[-1]
    Email = finaldf1.iloc[4, 0].split(" ")[-1]

    # Extractthemainbodyofstatement

    list1 = ['Transaction_Id', 'Date_time', 'Details', 'Status', 'Paid_in', 'Withdrawn', 'Balance']
    finaldf = []
    for df in dfs[1:]:
        finaldf.append(df)

    finaldf = pd.concat(finaldf)

    # datapreparationsteps
    finaldf1 = finaldf.drop(finaldf.columns[7], axis=1).rename(columns=finaldf.iloc[0]).drop(finaldf.index[0])
    finaldf1 = finaldf1.rename(columns=dict(zip(finaldf1.columns, list1)))

    pandadf1 = finaldf1
    pandadf1.to_csv(os.path.join(upload_folder_path, "def" + ".csv"), header=True, index=False)

    pandadf1["Amount"] = pandadf1['Withdrawn']
    pandadf1["Amount"] = pandadf1['Amount'].fillna(pandadf1['Withdrawn'].fillna(pandadf1['Paid_in']))
    pandadf1['Direction'] = np.where(pandadf1['Paid_in'].notnull(), 'Paid_in', 'Withdrawn')
    pandadf1["Customer_Name"] = Name
    pandadf1["Mobile"] = Mobile
    pandadf1["Email"] = Email
    pandadf1["date_parse"] = pandadf1["Date_time"]

    pandadf1 = pandadf1[['Transaction_Id', 'Customer_Name', 'date_parse', 'Details', 'Status', 'Amount', 'Balance', 'Direction', 'Mobile', 'Email']]

    pandadf1.to_excel(os.path.join(upload_folder_path, file_name + ".xlsx"), header=True, index=False)

    return file_name + ".xlsx"
