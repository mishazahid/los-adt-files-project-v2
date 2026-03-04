/************************************************************
 * CONFIGURATION
 ************************************************************/
const TEST_SHEET_ID      = '1FvZLxUS36JON-O8yY6zvrxxYyfOMHtHzmKAWUd5ytZk';
const SLIDES_TEMPLATE_ID = '1I-ELGBd0XsgRbhyrAJENej5vcX8xyTtq5iXB07rW_6M';
const DEST_FOLDER_ID     = '1DOThKA_GrOHzDZomzjOxnYfzCjVNWCql';

const DATA_SHEET_NAME     = 'Facility_Data'; // table + charts
const CHART_SHEET_NAME    = 'Facility_Data'; // charts on same sheet
const VARIABLE_SHEET_NAME = 'Summary';       // A=key, B=value

// Slide indices (0-based)
const CHART1_SLIDE_INDEX  = 0; // Chart 1 -> Slide 1
const TABLE_SLIDE_INDEX   = 1; // Table A-C -> Slide 2
const CHART23_SLIDE_INDEX = 2; // Charts 2 & 3 -> Slide 3

// Chart positions (points)
const CHART_POSITIONS = [
  { slideIndex: CHART1_SLIDE_INDEX,  left: 26.5,  top: 536, width: 648, height: 288 },
  { slideIndex: CHART23_SLIDE_INDEX, left: 31.68, top: 195, width: 648, height: 288 },
  { slideIndex: CHART23_SLIDE_INDEX, left: 31.68, top: 510, width: 648, height: 288 }
];

// Table layout
const TABLE_LEFT = 68;
const TABLE_TOP  = 97;
const TABLE_ALT_ROW_COLOR  = '#EEEEEE';
const TABLE_BASE_ROW_COLOR = '#FFFFFF';
const TABLE_TEXT_COLOR     = '#08206B';

/************************************************************
 * MAIN ENTRY POINT
 ************************************************************/
function createFacilityReport(comparisonMode) {
  const ss = SpreadsheetApp.openById(TEST_SHEET_ID); // Key: open by ID

  Logger.log(`createFacilityReport called with comparisonMode=${comparisonMode}`);

  // 1) Copy template and open
  const newFile = copyTemplate_();
  const presentation = SlidesApp.openById(newFile.getId());

  // 2) Replace placeholders
  replaceTextPlaceholders_(ss, presentation);

  // 3) Build dynamic table
  buildDynamicTable_(ss, presentation);

  // 4) Insert charts
  insertCharts_(ss, presentation);

  // Save before export
  presentation.saveAndClose();

  // 5) Export PDF to folder
  const pdfFile = exportPresentationAsPdfToFolder_(newFile, DEST_FOLDER_ID);

  Logger.log('Slides created: ' + newFile.getUrl());
  Logger.log('PDF created: ' + pdfFile.getUrl());

  return {
    success: true,
    message: 'Test Fac PDF generated successfully',
    pdf_link: pdfFile.getUrl(),
    file_id: pdfFile.getId(),
    file_name: pdfFile.getName(),
    folder_id: DEST_FOLDER_ID
  };
}

/************************************************************
 * COPY TEMPLATE
 ************************************************************/
function copyTemplate_() {
  const templateFile = DriveApp.getFileById(SLIDES_TEMPLATE_ID);
  const timestamp = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyyMMdd_HHmmss');
  const newName = templateFile.getName() + ' ' + timestamp;
  const destFolder = DriveApp.getFolderById(DEST_FOLDER_ID);
  return templateFile.makeCopy(newName, destFolder);
}

/************************************************************
 * PLACEHOLDERS FROM SHEET + DEFAULTS
 ************************************************************/

function getTextPlaceholders_(ss) {
  const placeholders = {};
  placeholders['{{REPORT_DATE}}'] = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM-dd');
  placeholders['{{TITLE}}'] = 'Facility Performance Report';

  const twoDecimalKeys = [
    '{{High SGG}}','{{High HD}}','{{Low SGG}}','{{2nd HD}}','{{3rd HD}}','{{4th HD}}',
    '{{High HT}}','{{2nd HT}}','{{3rd HT}}','{{4th HT}}','{{2nd SGG}}','{{3rd SGG}}','{{4th SGG}}',
    '{{Avg Man}}','{{Avg Med}}','{{Avg SGG}}'
  ];

  const varSheet = ss.getSheetByName(VARIABLE_SHEET_NAME);
  if (!varSheet) return placeholders;

  const lastRow = varSheet.getLastRow();
  const lastCol = varSheet.getLastColumn();
  if (lastRow < 1 || lastCol < 1) return placeholders;

  // A) Existing A:B key-value behavior
  if (lastRow >= 2) {
    const kv = varSheet.getRange(2, 1, lastRow - 1, 2).getValues();
    kv.forEach(([rawKey, rawVal]) => {
      if (!rawKey) return;
      let key = String(rawKey).trim().replace(/\s+/g, ' ');
      if (!key.startsWith('{{')) key = `{{${key}}}`;

      let formattedVal = '';
      if (rawVal !== null && rawVal !== undefined) {
        if (typeof rawVal === 'number' && twoDecimalKeys.includes(key)) formattedVal = rawVal.toFixed(2);
        else formattedVal = String(rawVal);
      }
      placeholders[key] = formattedVal;
    });
  }

  // B) Fallback: header row + first data row mapping (like your other script)
  if (lastRow >= 2) {
    const headerRow = varSheet.getRange(1, 1, 1, lastCol).getDisplayValues()[0];
    const dataRow   = varSheet.getRange(2, 1, 1, lastCol).getDisplayValues()[0];

    headerRow.forEach((h, i) => {
      const header = (h || '').toString().trim();
      if (!header) return;
      placeholders[`{{${header}}}`] = (dataRow[i] ?? '').toString();
    });
  }

  // C) Aliases for renamed/shortened placeholders
  const aliasToToken = {
    'Inj_Total': '{{Total Inj}}',
    'Total Injections': '{{Total Inj}}',
    'Patients Served (LTC)': '{{LTC Pat}}',
    'LTC Patients Served': '{{LTC Pat}}',
    // Map new procedure-name columns -> old CPT-code placeholders still in template
    'Inj_Small_Joint':    '{{Inj_20600}}',
    'Inj_Small_Joint_US': '{{Inj_20604}}',
    'Inj_Int_Joint':      '{{Inj_20605}}',
    'Inj_Int_Joint_US':   '{{Inj_20606}}',
    'Inj_Major_Joint':    '{{Inj_20610}}',
    'Inj_Major_Joint_US': '{{Inj_20611}}'
  };

  // D) Replace CPT label text in template with descriptive procedure names
  const cptLabelMap = {
    'CPT 20600': 'Small Joint Inj',
    'CPT 20604': 'Small Joint Inj w/US',
    'CPT 20605': 'Intermediate Joint Inj',
    'CPT 20606': 'Intermediate Joint Inj w/US',
    'CPT 20610': 'Major Joint Inj',
    'CPT 20611': 'Major Joint Inj w/US'
  };
  Object.entries(cptLabelMap).forEach(([oldLabel, newLabel]) => {
    placeholders[oldLabel] = newLabel;
  });

  Object.keys(aliasToToken).forEach(sourceKey => {
    const sourceToken = `{{${sourceKey}}}`;
    const targetToken = aliasToToken[sourceKey];
    if (!placeholders[targetToken] && placeholders[sourceToken] !== undefined) {
      placeholders[targetToken] = placeholders[sourceToken];
    }
  });

  return placeholders;
}

function replaceTextPlaceholders_(ss, presentation) {
  const map = getTextPlaceholders_(ss);
  Object.keys(map).forEach(key => {
    try {
      presentation.replaceAllText(key, map[key]);
    } catch (e) {
      Logger.log(`WARN: Could not replace "${key}": ${e}`);
      // Continue processing other placeholders
    }
  });
}

/************************************************************
 * BUILD DYNAMIC TABLE ON SLIDE 2
 ************************************************************/
function buildDynamicTable_(ss, presentation) {
  const dataSheet = ss.getSheetByName(DATA_SHEET_NAME);
  if (!dataSheet) throw new Error('Sheet "' + DATA_SHEET_NAME + '" not found.');
  const lastRow = dataSheet.getLastRow();
  if (lastRow < 2) throw new Error('Not enough data on sheet "' + DATA_SHEET_NAME + '".');

  const numRows = lastRow; // header + data
  const numCols = 3;       // columns A-C
  const data = dataSheet.getRange(1, 1, numRows, numCols).getValues();

  const slides = presentation.getSlides();
  if (TABLE_SLIDE_INDEX >= slides.length) throw new Error('Presentation does not have slide index ' + TABLE_SLIDE_INDEX);
  const slide = slides[TABLE_SLIDE_INDEX];

  const table = slide.insertTable(numRows, numCols);
  table.setLeft(TABLE_LEFT);
  table.setTop(TABLE_TOP);

  for (let r = 0; r < numRows; r++) {
    for (let c = 0; c < numCols; c++) {
      const cell = table.getCell(r, c);
      const value = data[r][c];
      const textRange = cell.getText();
      if (typeof value === 'number') textRange.setText(value.toFixed(2));
      else textRange.setText(value !== null ? String(value) : '');
      textRange.getTextStyle().setForegroundColor(TABLE_TEXT_COLOR);
      if (r === 0 || r === numRows - 1) textRange.getTextStyle().setBold(true);
    }
  }

  for (let r = 1; r < numRows; r++) {
    const isAlt = (r % 2 === 1);
    const rowColor = isAlt ? TABLE_ALT_ROW_COLOR : TABLE_BASE_ROW_COLOR;
    for (let c = 0; c < numCols; c++) {
      const cell = table.getCell(r, c);
      cell.getFill().setSolidFill(rowColor);
    }
  }
}

/************************************************************
 * INSERT CHARTS
 ************************************************************/
function insertCharts_(ss, presentation) {
  const sheet = ss.getSheetByName(CHART_SHEET_NAME);
  if (!sheet) throw new Error('Sheet "' + CHART_SHEET_NAME + '" not found.');
  const charts = sheet.getCharts();
  if (charts.length < 3) throw new Error('Expected at least 3 charts in sheet "' + CHART_SHEET_NAME + '" but only found ' + charts.length);

  const slides = presentation.getSlides();
  for (let i = 0; i < 3; i++) {
    const cfg = CHART_POSITIONS[i];
    if (cfg.slideIndex >= slides.length) throw new Error('Presentation does not have slide index ' + cfg.slideIndex);
    const slide = slides[cfg.slideIndex];
    const chartEl = slide.insertSheetsChartAsImage(charts[i]);
    chartEl.setLeft(cfg.left).setTop(cfg.top).setWidth(cfg.width).setHeight(cfg.height);
  }
}

/************************************************************
 * EXPORT PDF TO DRIVE FOLDER
 ************************************************************/
function exportPresentationAsPdfToFolder_(slidesFile, folderId) {
  const pdfBlob = slidesFile.getAs(MimeType.PDF);
  const folder = DriveApp.getFolderById(folderId);
  const pdfName = slidesFile.getName() + '.pdf';
  return folder.createFile(pdfBlob).setName(pdfName);
}

/*********************************
 * WEB APP HANDLERS
 *********************************/
function doPost(e) {
  try {
    const data = JSON.parse(e.postData.contents || '{}');
    const functionName = data.function || 'createFacilityReport';
    const comparisonMode = data.comparison_mode === true;

    Logger.log(`doPost called: function=${functionName}, comparison_mode=${comparisonMode}`);

    let result;
    if (functionName === 'createFacilityReport') {
      result = createFacilityReport(comparisonMode);
    } else {
      result = { success: false, error: 'Unknown function: ' + functionName };
    }
    return ContentService.createTextOutput(JSON.stringify(result))
      .setMimeType(ContentService.MimeType.JSON);
  } catch (error) {
    return ContentService.createTextOutput(JSON.stringify({
      success: false,
      error: error.toString()
    })).setMimeType(ContentService.MimeType.JSON);
  }
}
