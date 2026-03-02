/*********************************
 * CONFIG — UPDATE THESE VALUES
 *********************************/
const SHEET_NAME = 'Summary';                 // Tab with facility rows
const SLIDES_TEMPLATE_ID = '1R4EIGEjAJuHpaGUxHms1RkQe3c8BP1KbPZVmu2SrMJA'; // Facility Slides template ID
const OUTPUT_FOLDER_ID = '1DOThKA_GrOHzDZomzjOxnYfzCjVNWCql'; // Drive folder ID ('' to skip PDF export)
const TARGET_SLIDE_INDEX = 0;               // Slide index with the bars (0 = first slide)

// Bar width: convert inches to points (1 in = 72 pt). Example: 4 in -> 288.
const MAX_BAR_WIDTH_PTS = 241;             // Full width of your foreground bar at 100%

// Sheet column headers (must match your sheet exactly)
const HEADER_HOME     = '%HD';
const HEADER_HOSPITAL = '%HT';
const HEADER_SNF      = '%SNF';
const HEADER_EXPIRED  = '%Ex';
const HEADER_REHAB    = '%HDN';
const HEADER_HOSPICE  = '%Cus';
const HEADER_OBS      = '%AL';
const HEADER_OTHER    = '%OT';

// Alt text Titles on your foreground rectangles in Slides
const ALT_HOME     = 'BAR_HOME';
const ALT_HOSPITAL = 'BAR_HOSPITAL';
const ALT_SNF      = 'BAR_SNF';
const ALT_EXPIRED  = 'BAR_EXPIRED';
const ALT_REHAB    = 'BAR_HOME_NO';
const ALT_HOSPICE  = 'BAR_CUSTODIAL';
const ALT_OBS      = 'BAR_ASSISTED';
const ALT_OTHER    = 'BAR_OTHER';


/*********************************
 * MAIN ENTRY POINT — FACILITY DECKS
 *********************************/
function generateFacilitySlides(comparisonMode) {
  // Validate IDs and sheet
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(SHEET_NAME);
  if (!sheet) throw new Error(`Sheet tab "${SHEET_NAME}" not found.`);

  const templateFile = DriveApp.getFileById(SLIDES_TEMPLATE_ID); // throws if bad
  let outputFolder = null;
  if (OUTPUT_FOLDER_ID && OUTPUT_FOLDER_ID.trim() !== '') {
    outputFolder = DriveApp.getFolderById(OUTPUT_FOLDER_ID);     // throws if bad
  }

  // Read only populated range
  const lastRow = sheet.getLastRow();
  const lastCol = sheet.getLastColumn();
  if (lastRow < 2) throw new Error('No data rows found (need headers + at least 1 row).');

  const values = sheet.getRange(1, 1, lastRow, lastCol).getDisplayValues();
  const headers = values[0];
  const rows = values.slice(1).filter(r => (r[0] || '').toString().trim() !== '');

  if (rows.length === 0) throw new Error('All rows blank or missing Facility in column A.');

  Logger.log(`Found ${rows.length} facility rows. comparisonMode=${comparisonMode}`);

  rows.forEach((row, i) => {
    const facility = (row[0] || '').toString().trim();
    const copyName = facility ? `${facility} Report` : `Facility Report ${i + 1}`;
    Logger.log(`[${i + 1}/${rows.length}] Creating: ${copyName}`);

    // 1) Copy template and open
    const copyFile = templateFile.makeCopy(copyName);
    const pres = SlidesApp.openById(copyFile.getId());

    // 2) If comparison mode OFF, delete Slide 2 (Non-Puzzle) before replacing placeholders
    if (!comparisonMode) {
      const slides = pres.getSlides();
      if (slides.length > 1) {
        slides[1].remove();
        Logger.log(`  Removed Non-Puzzle slide (comparison mode OFF)`);
      }
    }

    // 3) Replace all {{Header}} placeholders across the deck
    headers.forEach((h, colIdx) => {
      const token = `{{${h}}}`;
      const val = (row[colIdx] ?? '').toString();
      pres.replaceAllText(token, val);
    });

    // 4) Resize the percentage bars on the target slide
    updateBarsForSlide_(pres, TARGET_SLIDE_INDEX, headers, row);

    pres.saveAndClose();

    // 5) Optional: export to PDF
    if (outputFolder) {
      const pdfBlob = copyFile.getAs('application/pdf');
      const pdfName = (facility ? facility : `Facility_${i + 1}`) + '.pdf';
      outputFolder.createFile(pdfBlob).setName(pdfName);
      // Optional: delete Slides copy to keep Drive clean:
      // copyFile.setTrashed(true);
    }
  });

  SpreadsheetApp.getActive().toast('Done! Check your Drive for generated Slides/PDFs.');
  Logger.log('All done.');
}


/*********************************
 * BAR UPDATE HELPERS
 *********************************/

/**
 * Resize the bar shapes by percentage, keeping LEFT edge fixed.
 */
function updateBarsForSlide_(presentation, slideIndex, headers, row) {
  const slide = presentation.getSlides()[slideIndex];

  const map = [
    { header: HEADER_HOME,     alt: ALT_HOME },
    { header: HEADER_HOSPITAL, alt: ALT_HOSPITAL },
    { header: HEADER_SNF,      alt: ALT_SNF },
    { header: HEADER_EXPIRED,  alt: ALT_EXPIRED },
    { header: HEADER_REHAB,    alt: ALT_REHAB },
    { header: HEADER_HOSPICE,  alt: ALT_HOSPICE },
    { header: HEADER_OBS,      alt: ALT_OBS },
    { header: HEADER_OTHER,    alt: ALT_OTHER }
  ];

  map.forEach(({ header, alt }) => {
    try {
      const colIdx = headers.indexOf(header);
      if (colIdx === -1) { Logger.log(`WARN: header "${header}" not found`); return; }
      const pct01 = normalizePercent_(row[colIdx]);
      const shape = findShapeByAltTitle_(slide, alt);
      if (!shape) { Logger.log(`WARN: shape "${alt}" not found`); return; }
      setBarWidth_(shape, pct01, MAX_BAR_WIDTH_PTS);
    } catch (e) {
      Logger.log(`WARN: bar ${alt}: ${e}`);
    }
  });
}


/**
 * Find a shape on a slide by its Alt text Title or Description (case-insensitive).
 * Recurses into groups so grouped bars still work.
 */
function findShapeByAltTitle_(slide, altTitle) {
  const target = (altTitle || '').trim().toLowerCase();
  const elements = slide.getPageElements();

  function check_(el) {
    const type = el.getPageElementType();
    if (type === SlidesApp.PageElementType.GROUP) {
      const kids = el.asGroup().getChildren();
      for (const k of kids) {
        const found = check_(k);
        if (found) return found;
      }
      return null;
    }
    if (type === SlidesApp.PageElementType.SHAPE) {
      const title = (el.getTitle() || '').trim().toLowerCase();
      const desc  = (el.getDescription ? (el.getDescription() || '') : '').trim().toLowerCase();
      if (title === target || desc === target) return el.asShape();
    }
    return null;
  }

  for (const el of elements) {
    const found = check_(el);
    if (found) return found;
  }
  return null;
}

/**
 * Normalize percent: accepts 61.5, "61.5%", 0.615, "0.615"; returns 0..1
 */
function normalizePercent_(raw) {
  if (raw === null || raw === undefined) return 0;
  let s = raw.toString().trim();
  if (s.endsWith('%')) s = s.slice(0, -1).trim();
  let num = Number(s);
  if (isNaN(num)) return 0;
  if (num > 1) num = num / 100;
  return Math.max(0, Math.min(1, num));
}

/**
 * SAFE RESIZER:
 * - clamps percentage
 * - ensures non-zero width & height
 * - temporarily clears rotation
 * - rebuilds the rectangle if resize still fails
 */
function setBarWidth_(shape, percent01, maxWidthPts) {
  const p = Math.max(0, Math.min(1, Number(percent01) || 0));
  const EPS = 0.1; // points
  const targetW = Math.max(EPS, maxWidthPts * p);

  const left   = shape.getLeft();
  const top    = shape.getTop();
  const height = Math.max(EPS, shape.getHeight());
  const rot    = shape.getRotation ? shape.getRotation() : 0;

  try {
    if (rot !== 0) shape.setRotation(0);
    shape.setHeight(height);
    shape.setWidth(targetW);
    shape.setLeft(left);
    shape.setTop(top);
    return;
  } catch (e1) {
    Logger.log(`WARN: direct resize failed (rot=${rot}). Attempting rebuild. Reason: ${e1}`);
  }

  try {
    const slide = shape.getParentPage();

    let hex = null;
    try {
      const color = shape.getFill().getSolidFill().getColor();
      if (color.asRgbColor) {
        hex = color.asRgbColor().asHexString();
      }
    } catch (_) {}

    const altTitle = shape.getTitle() || '';

    shape.remove();
    const newRect = slide.insertShape(
      SlidesApp.ShapeType.RECTANGLE,
      left, top, targetW, height
    );
    if (altTitle) newRect.setTitle(altTitle);
    if (hex) newRect.getFill().setSolidFill(hex);
    newRect.getLine().getLineFill().setTransparent();

    return;
  } catch (e2) {
    throw new Error(`Failed to rebuild bar at (${left}, ${top}) -> ${e2}`);
  }
}


/*********************************
 * DEBUG HELPER (optional)
 *********************************/
function debugListElementsOnTargetSlide() {
  const pres = SlidesApp.openById(SLIDES_TEMPLATE_ID);
  const slide = pres.getSlides()[TARGET_SLIDE_INDEX];

  Logger.log(`--- Elements on slide ${TARGET_SLIDE_INDEX} ---`);
  slide.getPageElements().forEach((el, i) => {
    const type = el.getPageElementType();
    const title = (el.getTitle() || '').replace(/\n/g, ' ');
    const desc  = (el.getDescription ? (el.getDescription() || '') : '').replace(/\n/g, ' ');
    const w = el.getWidth ? el.getWidth() : 'n/a';
    const h = el.getHeight ? el.getHeight() : 'n/a';
    Logger.log(`#${i+1} type=${type} title="${title}" desc="${desc}" w=${w} h=${h}`);
    if (type === SlidesApp.PageElementType.GROUP) {
      el.asGroup().getChildren().forEach((c, j) => {
        const ct = c.getPageElementType();
        const ctit = (c.getTitle() || '').replace(/\n/g, ' ');
        const cdesc = (c.getDescription ? (c.getDescription() || '') : '').replace(/\n/g, ' ');
        const cw = c.getWidth ? c.getWidth() : 'n/a';
        const ch = c.getHeight ? c.getHeight() : 'n/a';
        Logger.log(`  - child ${j+1} type=${ct} title="${ctit}" desc="${cdesc}" w=${cw} h=${ch}`);
      });
    }
  });
  Logger.log('--------------------------------------------');
}


/*********************************
 * EXECUTIVE SUMMARY GENERATOR
 *********************************/
const EXEC_SUMMARY_SHEET = 'Executive';
const EXEC_TEMPLATE_ID = '1tbxtx1Jq5nOZddw4TLpMOMXhDQNAu5lrHslAMeqBqR0';
const EXEC_OUTPUT_FOLDER_ID = '1DOThKA_GrOHzDZomzjOxnYfzCjVNWCql';

function generateExecutiveSummarySlide() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(EXEC_SUMMARY_SHEET);
  if (!sheet) throw new Error(`Tab "${EXEC_SUMMARY_SHEET}" not found.`);

  const values = sheet.getDataRange().getDisplayValues();
  const headers = values[0];
  const dataRow = values[1];

  const templateFile = DriveApp.getFileById(EXEC_TEMPLATE_ID);
  const copy = templateFile.makeCopy('Executive Summary');
  const pres = SlidesApp.openById(copy.getId());

  headers.forEach((h, i) => {
    const token = `{{${h}}}`;
    const val = (dataRow[i] ?? '').toString();
    pres.replaceAllText(token, val);
  });

  pres.saveAndClose();

  if (EXEC_OUTPUT_FOLDER_ID && EXEC_OUTPUT_FOLDER_ID.trim() !== '') {
    const folder = DriveApp.getFolderById(EXEC_OUTPUT_FOLDER_ID);
    const pdf = DriveApp.getFileById(copy.getId()).getAs('application/pdf');
    folder.createFile(pdf).setName('Executive Summary.pdf');
  }

  SpreadsheetApp.getActive().toast('Executive Summary slide generated!');
  Logger.log('Executive Summary created successfully.');
}


/*********************************
 * COMBINED PDF GENERATOR (10x11 SAFE)
 *********************************/

/**
 * Build ONE combined PDF: Executive Summary first, then one slide per facility.
 * Page size stays 10"x11" because combined deck is a copy of facility template.
 *
 * @param {boolean} comparisonMode - When true, include Non-Puzzle slide (slide 1)
 *                                   for each facility alongside the Puzzle slide (slide 0).
 */
function generatePDF(comparisonMode) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();

  const summarySheet = ss.getSheetByName(SHEET_NAME);
  if (!summarySheet) throw new Error(`Sheet tab "${SHEET_NAME}" not found.`);

  const execSheet = ss.getSheetByName(EXEC_SUMMARY_SHEET);
  if (!execSheet) throw new Error(`Tab "${EXEC_SUMMARY_SHEET}" not found.`);

  const execTemplateFile = DriveApp.getFileById(EXEC_TEMPLATE_ID);
  const facilityTemplateFile = DriveApp.getFileById(SLIDES_TEMPLATE_ID);

  const outputFolder = (OUTPUT_FOLDER_ID && OUTPUT_FOLDER_ID.trim() !== '')
    ? DriveApp.getFolderById(OUTPUT_FOLDER_ID)
    : null;

  if (!outputFolder) {
    throw new Error('OUTPUT_FOLDER_ID is empty — please set a Drive folder to receive the combined PDF.');
  }

  Logger.log(`generatePDF called with comparisonMode=${comparisonMode}`);

  // ---- Read Executive Summary data ----
  const execVals = execSheet.getDataRange().getDisplayValues();
  const execHeaders = execVals[0];
  const execRow = execVals[1] || [];
  if (execHeaders.length === 0) throw new Error('Executive sheet has no headers.');
  if (execVals.length < 2) throw new Error('Executive sheet needs at least one data row.');

  // ---- Read facility rows ----
  const lastRow = summarySheet.getLastRow();
  const lastCol = summarySheet.getLastColumn();
  if (lastRow < 2) throw new Error('No facility data rows found.');

  const allValues = summarySheet.getRange(1, 1, lastRow, lastCol).getDisplayValues();
  const headers = allValues[0];
  const rows = allValues.slice(1).filter(r => (r[0] || '').toString().trim() !== '');
  if (rows.length === 0) throw new Error('All rows blank or missing Facility in column A.');

  // Create combined deck as COPY of facility template (inherits 10x11)
  const combinedName = `Combined Report \u2013 ${new Date().toISOString().slice(0,10)}`;
  const combinedFile = facilityTemplateFile.makeCopy(combinedName);
  const combinedId = combinedFile.getId();
  let combined = SlidesApp.openById(combinedId);

  // Wipe slides to start clean
  combined.getSlides().forEach(s => s.remove());

  const tempToTrash = [];

  // ---------- 1) Build Executive Summary temp deck ----------
  const execCopy = execTemplateFile.makeCopy('TMP \u2013 Executive Summary');
  tempToTrash.push(execCopy);
  const execPres = SlidesApp.openById(execCopy.getId());

  execHeaders.forEach((h, i) => {
    const token = `{{${h}}}`;
    const val = (execRow[i] ?? '').toString();
    execPres.replaceAllText(token, val);
  });
  execPres.saveAndClose();

  // Insert all exec slides at the start
  const execSlides = SlidesApp.openById(execCopy.getId()).getSlides();
  for (let i = execSlides.length - 1; i >= 0; i--) {
    combined.insertSlide(0, execSlides[i]);
  }
  Logger.log(`Inserted ${execSlides.length} summary slide(s).`);

  // ---------- 2) Facility slides ----------
  rows.forEach((row, i) => {
    const facility = (row[0] || '').toString().trim();
    const copyName = facility ? `TMP \u2013 ${facility}` : `TMP \u2013 Facility ${i + 1}`;

    const facCopy = facilityTemplateFile.makeCopy(copyName);
    tempToTrash.push(facCopy);
    const facPres = SlidesApp.openById(facCopy.getId());

    // If comparison mode OFF, delete Slide 2 (Non-Puzzle) before replacing placeholders
    if (!comparisonMode) {
      const facSlides = facPres.getSlides();
      if (facSlides.length > 1) {
        facSlides[1].remove();
        Logger.log(`  Removed Non-Puzzle slide for "${facility}" (comparison mode OFF)`);
      }
    }

    headers.forEach((h, colIdx) => {
      const token = `{{${h}}}`;
      const val = (row[colIdx] ?? '').toString();
      facPres.replaceAllText(token, val);
    });

    updateBarsForSlide_(facPres, TARGET_SLIDE_INDEX, headers, row);
    facPres.saveAndClose();

    const srcSlides = SlidesApp.openById(facCopy.getId()).getSlides();

    // Append Slide 1 (Puzzle — always)
    const puzzleSlide = srcSlides[TARGET_SLIDE_INDEX];
    if (puzzleSlide) {
      combined.appendSlide(puzzleSlide);
      Logger.log(`Appended Puzzle slide for "${facility || ('Facility ' + (i+1))}".`);
    } else {
      Logger.log(`WARN: No slide at TARGET_SLIDE_INDEX=${TARGET_SLIDE_INDEX} for ${copyName}`);
    }

    // Append Slide 2 (Non-Puzzle — only when comparison mode ON)
    if (comparisonMode && srcSlides.length > 1) {
      combined.appendSlide(srcSlides[1]);
      Logger.log(`Appended Non-Puzzle slide for "${facility || ('Facility ' + (i+1))}".`);
    }
  });

  combined.saveAndClose();

  // ---------- 3) Export combined to PDF ----------
  const combinedPdf = DriveApp.getFileById(combinedId).getAs('application/pdf');
  const pdfFile = outputFolder.createFile(combinedPdf).setName(`${combinedName}.pdf`);
  Logger.log(`PDF created: ${pdfFile.getName()}`);
  const pdfUrl = pdfFile.getUrl();
  const pdfId = pdfFile.getId();

  // ---------- 4) Cleanup temp decks ----------
  try {
    tempToTrash.forEach(f => f.setTrashed(true));
  } catch (e) {
    Logger.log(`Cleanup warning: ${e}`);
  }

  SpreadsheetApp.getActive().toast('Combined PDF generated at 10x11 size.');

  // Return details for Web App / API callers
  return {
    success: true,
    pdf_link: pdfUrl,
    file_id: pdfId,
    file_name: `${combinedName}.pdf`,
    folder_id: OUTPUT_FOLDER_ID
  };
}

/*********************************
 * WEB APP HANDLERS
 *********************************/
function doPost(e) {
  try {
    const data = e && e.postData && e.postData.contents ? JSON.parse(e.postData.contents) : {};
    const fn = data.function || 'generatePDF';
    const comparisonMode = data.comparison_mode === true;

    Logger.log(`doPost called: function=${fn}, comparison_mode=${comparisonMode}`);

    let result;
    if (fn === 'generatePDF') {
      result = generatePDF(comparisonMode);
    } else if (fn === 'generateFacilitySlides') {
      result = generateFacilitySlides(comparisonMode);
    } else if (fn === 'generateExecutiveSummarySlide') {
      result = generateExecutiveSummarySlide();
    } else {
      result = { success: false, error: `Unknown function: ${fn}` };
    }

    return ContentService.createTextOutput(JSON.stringify(result))
      .setMimeType(ContentService.MimeType.JSON);
  } catch (err) {
    return ContentService.createTextOutput(JSON.stringify({
      success: false,
      error: err.toString()
    })).setMimeType(ContentService.MimeType.JSON);
  }
}
