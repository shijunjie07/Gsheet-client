import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from df2gspread import df2gspread as d2g

class GsheetClient:
    """
    Google Sheet Client
    
    """
    # initiation
    def __init__(self):
        scope = ['https://www.googleapis.com/auth/spreadsheets',
            "https://www.googleapis.com/auth/drive"]

        credential_loc = os.environ['gs_credentials']       # replace with your .json
        self.credentials = ServiceAccountCredentials.from_json_keyfile_name(credential_loc, scope)
        self.client = gspread.authorize(self.credentials)
        print(credential_loc)

    def sheet_id(self, sheet_name:str):
        """Return Spreadsheet ID"""
        return self.client.open(sheet_name).id;
    
    def pull_gsheet(self, sheet_name:str, sheet_index:int):
        """
        Read from google spreadsheet

        :param sheet_name: name of the spreadsheet
        :param sheet_index: An index of a worksheet. Indexes start from zero.
        :type sheet_name: str
        :type sheet_index: int
        
        :returns: a pandas dataframe contains data from Gspreadsheet
        """
        sh = self.client.open(sheet_name)       # open sheet

        sh_instance = sh.get_worksheet(sheet_index) 

        data = sh_instance.get_all_records()    # get data

        df = pd.DataFrame.from_dict(data)       # turn into a DF

        return df

    def push_2_gsheet(
        self, df:pd.DataFrame, sheet_name:str,
        wks_name:str, share_with:list[str]=None, perm_type:str=None, 
        role:str=None, email_msg:str=None, 
        ):        
        """
        Push a pandas dataframe to spreadsheet

        :param df: pandas dataframe
        :param sheet_name: name of the spreadsheet
        :param wks_name: worksheet name
        (optional)
        :param share_with: list of email address u want to share with
        :param perm_type: The account type.
               Allowed values are: ``user``, ``group``, ``domain``,
               ``anyone``.
        :param role: The primary role for this user.
               Allowed values are: ``owner``, ``writer``, ``reader``.
        :param email_msg: The email to be sent if notify=True
        :type df: pandas.DataFrame
        :type sheet_name: str
        :type wks_name: str
        :type share_with: [str]
        :type perm_type: str
        :type role: str
        :type email_msg: str
        """

        try:
            sh = self.client.open(sheet_name)
        except:
            sh = self.client.create(sheet_name)

        sprd_key = sh.id    # spreadsheet ID

        # update df
        d2g.upload(df, sprd_key, wks_name, credentials=self.credentials, row_names=True)
        print(f"DF uploaded to {sheet_name}_{wks_name}!")
        # share df with other users
        if (share_with):
            sh.share(share_with, perm_type, role, email_message=email_msg)
            share_str = ', '.join(share_with)
            print(f"Spreadsheet shared with {share_str}")
