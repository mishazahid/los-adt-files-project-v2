"""Create Slide 2 (Non-Puzzle Providers Comparison) in Template 2"""
from google.oauth2 import service_account
from googleapiclient.discovery import build
import uuid

creds = service_account.Credentials.from_service_account_file(
    'credentials.json',
    scopes=['https://www.googleapis.com/auth/presentations']
)
slides_service = build('slides', 'v1', credentials=creds)
pres_id = '1R4EIGEjAJuHpaGUxHms1RkQe3c8BP1KbPZVmu2SrMJA'

# Read to get slide dimensions
pres = slides_service.presentations().get(presentationId=pres_id).execute()
page_w = pres.get('pageSize', {}).get('width', {}).get('magnitude', 9144000)
page_h = pres.get('pageSize', {}).get('height', {}).get('magnitude', 6858000)
print(f"Page size: {page_w} x {page_h}")

def emu(val):
    return {'magnitude': val, 'unit': 'EMU'}

def uid():
    return 'np_' + uuid.uuid4().hex[:12]

slide2_id = uid()
requests = []

# 1. Create new slide after slide 1
requests.append({
    'createSlide': {
        'objectId': slide2_id,
        'insertionIndex': 1,
    }
})

def create_textbox(obj_id, x, y, w, h, text, font_size=9, bold_title=None):
    reqs = []
    reqs.append({
        'createShape': {
            'objectId': obj_id,
            'shapeType': 'TEXT_BOX',
            'elementProperties': {
                'pageObjectId': slide2_id,
                'size': {'width': emu(w), 'height': emu(h)},
                'transform': {
                    'scaleX': 1, 'scaleY': 1,
                    'translateX': x, 'translateY': y,
                    'unit': 'EMU'
                }
            }
        }
    })
    reqs.append({
        'insertText': {
            'objectId': obj_id,
            'text': text,
            'insertionIndex': 0
        }
    })
    reqs.append({
        'updateTextStyle': {
            'objectId': obj_id,
            'textRange': {'type': 'ALL'},
            'style': {
                'fontSize': {'magnitude': font_size, 'unit': 'PT'},
                'fontFamily': 'Arial'
            },
            'fields': 'fontSize,fontFamily'
        }
    })
    if bold_title:
        reqs.append({
            'updateTextStyle': {
                'objectId': obj_id,
                'textRange': {'type': 'FIXED_RANGE', 'startIndex': 0, 'endIndex': len(bold_title)},
                'style': {
                    'bold': True,
                    'fontSize': {'magnitude': font_size + 2, 'unit': 'PT'}
                },
                'fields': 'bold,fontSize'
            }
        })
    return reqs

# ============================================================
# SLIDE 2 LAYOUT: Non-Puzzle Providers Comparison
# ============================================================

# Title
title_id = uid()
requests += create_textbox(title_id, 500000, 200000, 8000000, 500000,
    '{{Facility}} \u2014 Non-Puzzle Providers', font_size=18,
    bold_title='{{Facility}} \u2014 Non-Puzzle Providers')

# Subtitle
sub_id = uid()
requests += create_textbox(sub_id, 500000, 650000, 8000000, 300000,
    'Comparison metrics for non-Puzzle providers at this facility', font_size=9)

# --- LEFT COLUMN ---

# Encounter Metrics
enc_id = uid()
enc_text = 'Encounter Metrics\nPatients Served: {{NP_Patients Served}}'
requests += create_textbox(enc_id, 500000, 1100000, 3800000, 600000,
    enc_text, font_size=10, bold_title='Encounter Metrics')

# Stay Duration Analysis label
stay_label_id = uid()
requests += create_textbox(stay_label_id, 500000, 1900000, 3800000, 300000,
    'Stay Duration Analysis', font_size=12, bold_title='Stay Duration Analysis')

# Stay Duration Table
table_id = uid()
requests.append({
    'createTable': {
        'objectId': table_id,
        'elementProperties': {
            'pageObjectId': slide2_id,
            'size': {'width': emu(3800000), 'height': emu(1200000)},
            'transform': {
                'scaleX': 1, 'scaleY': 1,
                'translateX': 500000, 'translateY': 2200000,
                'unit': 'EMU'
            }
        },
        'rows': 4,
        'columns': 2
    }
})

table_data = [
    (0, 0, 'Payor Type'), (0, 1, 'Average LOS'),
    (1, 0, 'Overall'), (1, 1, '{{NP_LOS Overall Avg}} days'),
    (2, 0, 'Managed Care'), (2, 1, '{{NP_Managed Care Ratio}} \u2014 {{NP_LOS Man Avg}} days'),
    (3, 0, 'Medicare A'), (3, 1, '{{NP_Medicare A Ratio}} \u2014 {{NP_LOS Med Avg}} days'),
]
for row, col, text in table_data:
    requests.append({
        'insertText': {
            'objectId': table_id,
            'cellLocation': {'rowIndex': row, 'columnIndex': col},
            'text': text,
            'insertionIndex': 0
        }
    })

# Style table header
for col in [0, 1]:
    requests.append({
        'updateTextStyle': {
            'objectId': table_id,
            'cellLocation': {'rowIndex': 0, 'columnIndex': col},
            'textRange': {'type': 'ALL'},
            'style': {'bold': True, 'fontSize': {'magnitude': 9, 'unit': 'PT'}, 'fontFamily': 'Arial'},
            'fields': 'bold,fontSize,fontFamily'
        }
    })
for row in [1, 2, 3]:
    for col in [0, 1]:
        requests.append({
            'updateTextStyle': {
                'objectId': table_id,
                'cellLocation': {'rowIndex': row, 'columnIndex': col},
                'textRange': {'type': 'ALL'},
                'style': {'fontSize': {'magnitude': 9, 'unit': 'PT'}, 'fontFamily': 'Arial'},
                'fields': 'fontSize,fontFamily'
            }
        })

# Discharge Destinations label
dd_label_id = uid()
requests += create_textbox(dd_label_id, 500000, 3600000, 3800000, 300000,
    'Discharge Destinations', font_size=12, bold_title='Discharge Destinations')

# Discharge grid - left col and right col
dd_items = [
    # Left column
    (500000, 3950000, '{{NP_%HD}}% ({{NP_HD}})', 'Home Discharge w/ Services'),
    (500000, 4500000, '{{NP_%HDN}}% ({{NP_HDN}})', 'Home Discharge w/o Services'),
    (500000, 5050000, '{{NP_%Cus}}% ({{NP_Cus}})', 'Custodial'),
    (500000, 5600000, '{{NP_%SNF}}% ({{NP_SNF}})', 'SNF Transfer'),
    # Right column
    (2500000, 3950000, '{{NP_%HT}}% ({{NP_HT}})', 'Hospital Transfer'),
    (2500000, 4500000, '{{NP_%Ex}}% ({{NP_Ex}})', 'Expired'),
    (2500000, 5050000, '{{NP_%AL}}% ({{NP_AL}})', 'Assisted Living'),
    (2500000, 5600000, '{{NP_%OT}}% ({{NP_OT}})', 'Other'),
]

for x, y, pct_text, label in dd_items:
    pct_id = uid()
    requests += create_textbox(pct_id, x, y, 1800000, 250000,
        pct_text, font_size=11, bold_title=pct_text)
    lbl_id = uid()
    requests += create_textbox(lbl_id, x, y + 260000, 1800000, 200000,
        label, font_size=8)

# --- RIGHT COLUMN ---

# Functional Gains
fg_id = uid()
fg_text = ('Functional Gains\n\n'
           '5-Day Average: {{NP_GS}}\n'
           'End-of-PPS Average: {{NP_PPS}}\n'
           'Section GG Score Increase: {{NP_INC}}\n\n'
           'Avg. GG Gain Managed Care: {{NP_GG_Gain_MC}}\n'
           'Avg. GG Gain Medicare A: {{NP_GG_Gain_MA}}\n'
           'Avg. GG Gain Overall: {{NP_GG_Gain_Overall}}')
requests += create_textbox(fg_id, 4800000, 1100000, 4000000, 2200000,
    fg_text, font_size=10, bold_title='Functional Gains')

# Note about Injections
note_id = uid()
note_text = 'Note: Performed Injections are Puzzle-specific and are shown on the facility slide only.'
requests += create_textbox(note_id, 4800000, 3600000, 4000000, 400000,
    note_text, font_size=8)

print(f"Sending {len(requests)} requests to create Slide 2...")
result = slides_service.presentations().batchUpdate(
    presentationId=pres_id,
    body={'requests': requests}
).execute()
print(f"Done! Slide 2 created with {len(result.get('replies', []))} operations.")
