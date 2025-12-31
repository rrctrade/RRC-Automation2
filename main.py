/******************************************************
 RajanTradeAutomation – WebApp.gs (v3.3 FINAL FIXED)
 ✔ Settings time → always HH:MM:SS
 ✔ Logs sheet support
 ✔ Safe for Python (Render)
******************************************************/

/* ================= BASIC ================= */

function doGet() {
  return ContentService.createTextOutput("OK");
}

function doPost(e) {
  try {
    const body = JSON.parse(e.postData?.contents || "{}");
    const action = body.action;
    const payload = body.payload || {};

    switch (action) {

      case "ping":
        return out_({ ok: true });

      case "getSettings":
        return out_({ ok: true, settings: readSettings_() });

      case "pushCandle":
        push_("CandleHistory", payload.candles);
        return out_({ ok: true });

      case "pushCandleEngine":
        push_("LiveCandlesEngine", payload.candles);
        return out_({ ok: true });

      case "pushSignal":
        push_("Signals", payload.signals);
        return out_({ ok: true });

      case "pushState":
        pushState_(payload.items);
        return out_({ ok: true });

      /* 🔥 LOGS FROM main.py */
      case "pushLog":
        push_("Logs", payload.rows);
        return out_({ ok: true });

      default:
        return out_({ ok: false, error: "Unknown action" });
    }

  } catch (err) {
    logError_("doPost", err);
    return out_({ ok: false, error: String(err) });
  }
}

/* ====================================================
   SETTINGS (🔥 TIME FIXED HERE 🔥)
   ==================================================== */
function readSettings_() {
  const sh = getSheet_("Settings");
  if (!sh) return {};

  const map = {};
  const lastRow = sh.getLastRow();
  if (lastRow < 2) return map;

  const tz = "Asia/Kolkata";

  sh.getRange(2, 1, lastRow - 1, 2)
    .getValues()
    .forEach(row => {
      const key = row[0];
      let val = row[1];

      if (!key) return;

      // ⏰ If value is Date (time cell) → convert to HH:MM:SS
      if (val instanceof Date) {
        val = Utilities.formatDate(val, tz, "HH:mm:ss");
      }

      map[key] = String(val);
    });

  return map;
}

/* ====================================================
   GENERIC PUSH (APPEND SAFE)
   ==================================================== */
function push_(sheetName, rows) {
  if (!rows || !rows.length) return;

  const sh = getSheet_(sheetName);
  sh.getRange(
    sh.getLastRow() + 1,
    1,
    rows.length,
    rows[0].length
  ).setValues(rows);
}

/* ====================================================
   STATE (KEY–VALUE UPSERT)
   ==================================================== */
function pushState_(items) {
  if (!items || !items.length) return;

  const sh = getSheet_("State");
  const lastRow = sh.getLastRow();
  const existing = {};

  if (lastRow > 1) {
    sh.getRange(2, 1, lastRow - 1, 2)
      .getValues()
      .forEach((r, i) => {
        if (r[0]) existing[r[0]] = i + 2;
      });
  }

  items.forEach(it => {
    if (!it.key) return;

    if (existing[it.key]) {
      sh.getRange(existing[it.key], 2).setValue(it.value);
    } else {
      sh.appendRow([it.key, it.value]);
    }
  });
}

/* ====================================================
   HELPERS
   ==================================================== */
function getSheet_(name) {
  const ss = SpreadsheetApp.getActive();
  let sh = ss.getSheetByName(name);

  if (!sh) {
    sh = ss.insertSheet(name);
  }
  return sh;
}

function out_(obj) {
  return ContentService
    .createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}

/* ====================================================
   LOGGING (SAFE)
   ==================================================== */
function logError_(fn, err) {
  try {
    const sh = getSheet_("Logs");
    sh.appendRow([new Date(), "ERROR", fn + " : " + err]);
  } catch (e) {}
}
