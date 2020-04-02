import json
import os.path
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def get_credentials(input_credentials):
    """
    Takes the input param 'credentials' that can accept a JSON token or a path to a file
    and returns a dict.
    """
    test_file = input_credentials.splitlines()[0]
    if os.path.isfile(test_file):
        try:
            with open(test_file, 'r') as f:
                credentials = json.load(f)
                f.close()
        except Exception as e:
            raise ValueError("Unable to read the JSON Service Account from file '%s'.\n%s" % (test_file, e))
    else:
        try:
            credentials = json.loads(input_credentials)
        except Exception as e:
            raise Exception("Unable to read the JSON Service Account.\n%s" % e)

    return credentials

def get_spreadsheet(credentials, doc_id, tab_id):
    """
    Inputs params:
    * credentials
    * doc_id
    * tab_id
    Returns a gspread's worksheet object.
    """
    credentials = get_credentials(credentials)
    scope = [
        'https://www.googleapis.com/auth/spreadsheets'
    ]
    gspread_client = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(credentials, scope))

    try:
        return gspread_client.open_by_key(doc_id).worksheet(tab_id)
    except gspread.exceptions.SpreadsheetNotFound as e:
        raise Exception("Trying to open non-existent or inaccessible spreadsheet document.")
    except gspread.exceptions.WorksheetNotFound as e:
        raise Exception("Trying to open non-existent sheet. Verify that the sheet name exists (%s)." % tab_id)
    except gspread.exceptions.APIError as e:
        if hasattr(e, 'response'):
            error_json = e.response.json()
            print(error_json)
            error_status = error_json.get("error", {}).get("status")
            email = credentials.get("client_email", "(email missing)")
            if error_status == 'PERMISSION_DENIED':
                raise Exception("The Service Account does not have permission to read or write on the spreadsheet document. Have you shared the spreadsheet with %s?" % email)
            if error_status == 'NOT_FOUND':
                raise Exception("Trying to open non-existent spreadsheet document. Verify the document id exists (%s)." % doc_id)
        raise Exception("The Google API returned an error: %s" % e)
