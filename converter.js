/* XLSX -> BigQuery-ready CSV converter (runs in a Web Worker, no external libraries).
 * Streams the sheet XML so very large files (hundreds of MB unpacked) fit in browser memory.
 * Pass 1 (analyse): read headers, profile every column, decide BigQuery types.
 * Pass 2 (build):   write the cleaned CSV (+ gzip) using the final column names/types.
 */
'use strict';

/* ---------------- ZIP reading ---------------- */
async function readZipIndex(file) {
  const tailLen = Math.min(file.size, 65557 + 22);
  const tail = new DataView(await file.slice(file.size - tailLen).arrayBuffer());
  let eocd = -1;
  for (let i = tail.byteLength - 22; i >= 0; i--) {
    if (tail.getUint32(i, true) === 0x06054b50) { eocd = i; break; }
  }
  if (eocd < 0) throw new Error('This file is not a valid .xlsx (zip) file. Old .xls files must be saved as .xlsx first.');
  let cdCount = tail.getUint16(eocd + 10, true);
  let cdSize = tail.getUint32(eocd + 12, true);
  let cdOffset = tail.getUint32(eocd + 16, true);
  if (cdOffset === 0xFFFFFFFF || cdCount === 0xFFFF) {
    // zip64
    const locPos = eocd - 20;
    if (locPos >= 0 && tail.getUint32(locPos, true) === 0x07064b50) {
      const z64Off = Number(tail.getBigUint64(locPos + 8, true));
      const z = new DataView(await file.slice(z64Off, z64Off + 56).arrayBuffer());
      cdCount = Number(z.getBigUint64(32, true));
      cdSize = Number(z.getBigUint64(40, true));
      cdOffset = Number(z.getBigUint64(48, true));
    }
  }
  const cd = new DataView(await file.slice(cdOffset, cdOffset + cdSize).arrayBuffer());
  const dec = new TextDecoder();
  const entries = {};
  let p = 0;
  for (let n = 0; n < cdCount && p + 46 <= cd.byteLength; n++) {
    if (cd.getUint32(p, true) !== 0x02014b50) break;
    const method = cd.getUint16(p + 10, true);
    let compSize = cd.getUint32(p + 20, true);
    let size = cd.getUint32(p + 24, true);
    const nameLen = cd.getUint16(p + 28, true);
    const extraLen = cd.getUint16(p + 30, true);
    const commentLen = cd.getUint16(p + 32, true);
    let localOff = cd.getUint32(p + 42, true);
    const name = dec.decode(new Uint8Array(cd.buffer, p + 46, nameLen));
    // zip64 extra field
    let e = p + 46 + nameLen; const eEnd = e + extraLen;
    while (e + 4 <= eEnd) {
      const id = cd.getUint16(e, true), len = cd.getUint16(e + 2, true);
      if (id === 0x0001) {
        let q = e + 4;
        if (size === 0xFFFFFFFF) { size = Number(cd.getBigUint64(q, true)); q += 8; }
        if (compSize === 0xFFFFFFFF) { compSize = Number(cd.getBigUint64(q, true)); q += 8; }
        if (localOff === 0xFFFFFFFF) { localOff = Number(cd.getBigUint64(q, true)); q += 8; }
      }
      e += 4 + len;
    }
    entries[name.replace(/\\/g, '/')] = { method, compSize, size, localOff };
    p += 46 + nameLen + extraLen + commentLen;
  }
  return entries;
}

async function entryStream(file, entry, onBytes) {
  const lh = new DataView(await file.slice(entry.localOff, entry.localOff + 30).arrayBuffer());
  if (lh.getUint32(0, true) !== 0x04034b50) throw new Error('Corrupt zip entry');
  const start = entry.localOff + 30 + lh.getUint16(26, true) + lh.getUint16(28, true);
  let s = file.slice(start, start + entry.compSize).stream();
  if (onBytes) {
    s = s.pipeThrough(new TransformStream({ transform(chunk, ctl) { onBytes(chunk.byteLength); ctl.enqueue(chunk); } }));
  }
  if (entry.method === 8) s = s.pipeThrough(new DecompressionStream('deflate-raw'));
  else if (entry.method !== 0) throw new Error('Unsupported zip compression method ' + entry.method);
  return s.pipeThrough(new TextDecoderStream());
}

async function entryText(file, entries, name) {
  const ent = entries[name];
  if (!ent) return null;
  const r = (await entryStream(file, ent)).getReader();
  let out = '';
  for (;;) { const { value, done } = await r.read(); if (done) break; out += value; }
  return out;
}

/* Stream text and hand complete segments (ending with closeTag) to onSegment. */
async function scanSegments(stream, closeTag, onSegment) {
  const reader = stream.getReader();
  let buf = '';
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    buf += value;
    const cut = buf.lastIndexOf(closeTag);
    if (cut >= 0) {
      const end = cut + closeTag.length;
      const stop = onSegment(buf.slice(0, end));
      buf = buf.slice(end);
      if (stop === true) { try { await reader.cancel(); } catch (e) {} return; }
    }
  }
  if (buf.length) onSegment(buf);
}

/* ---------------- XML helpers ---------------- */
function unescapeXml(s) {
  if (s.indexOf('&') >= 0) {
    s = s.replace(/&(#x[0-9a-fA-F]+|#\d+|amp|lt|gt|quot|apos);/g, (m, g) => {
      switch (g) {
        case 'amp': return '&'; case 'lt': return '<'; case 'gt': return '>';
        case 'quot': return '"'; case 'apos': return "'";
        default: return g[1] === 'x' ? String.fromCodePoint(parseInt(g.slice(2), 16)) : String.fromCodePoint(parseInt(g.slice(1), 10));
      }
    });
  }
  if (s.indexOf('_x') >= 0) s = s.replace(/_x([0-9A-Fa-f]{4})_/g, (m, h) => String.fromCharCode(parseInt(h, 16)));
  return s;
}
function textOfRich(inner) {
  // concatenate <t> contents, ignoring phonetic runs
  if (inner.indexOf('<rPh') >= 0) inner = inner.replace(/<rPh\b[\s\S]*?<\/rPh>/g, '');
  let out = '';
  const re = /<t(?:\s[^>]*)?>([\s\S]*?)<\/t>|<t(?:\s[^>]*)?\/>/g;
  let m;
  while ((m = re.exec(inner))) if (m[1]) out += m[1];
  return unescapeXml(out);
}
function attr(s, name) {
  const re = new RegExp('(?:^|\\s)' + name + '="([^"]*)"');
  const m = re.exec(s);
  return m ? m[1] : null;
}
function colIndex(ref) {
  let n = 0;
  for (let i = 0; i < ref.length; i++) {
    const c = ref.charCodeAt(i);
    if (c >= 65 && c <= 90) n = n * 26 + (c - 64);
    else break;
  }
  return n - 1;
}

/* ---------------- workbook metadata ---------------- */
async function openWorkbook(file) {
  const entries = await readZipIndex(file);
  const wbXml = await entryText(file, entries, 'xl/workbook.xml');
  if (!wbXml) throw new Error('No xl/workbook.xml inside — is this really an .xlsx file?');
  const relsXml = (await entryText(file, entries, 'xl/_rels/workbook.xml.rels')) || '';
  const rels = {};
  relsXml.replace(/<Relationship\b([^>]*)\/?>/g, (m, a) => {
    rels[attr(a, 'Id')] = attr(a, 'Target');
    return '';
  });
  const sheets = [];
  wbXml.replace(/<sheet\b([^>]*)\/?>/g, (m, a) => {
    const rid = attr(a, 'r:id') || (a.match(/\s\w+:id="([^"]*)"/) || [])[1];
    let target = rels[rid] || '';
    target = target.startsWith('/') ? target.slice(1) : 'xl/' + target.replace(/^\.\//, '');
    const ent = entries[target];
    sheets.push({ name: unescapeXml(attr(a, 'name') || ''), path: target, size: ent ? ent.size : 0, state: attr(a, 'state') || 'visible' });
    return '';
  });
  const date1904 = /date1904="(1|true)"/.test(wbXml);
  // styles: which style indexes are date/time formats
  const stylesXml = (await entryText(file, entries, 'xl/styles.xml')) || '';
  const customFmts = {};
  stylesXml.replace(/<numFmt\b([^>]*)\/?>/g, (m, a) => { customFmts[attr(a, 'numFmtId')] = unescapeXml(attr(a, 'formatCode') || ''); return ''; });
  const dateStyles = [];
  const cellXfs = (stylesXml.match(/<cellXfs\b[\s\S]*?<\/cellXfs>/) || [''])[0];
  let idx = 0;
  cellXfs.replace(/<xf\b([^>]*?)(\/>|>)/g, (m, a) => {
    const id = parseInt(attr(a, 'numFmtId') || '0', 10);
    dateStyles[idx++] = fmtKind(id, customFmts[id]);
    return '';
  });
  return { entries, sheets, date1904, dateStyles };
}
// returns 0 = not a date format, 1 = date/datetime format, 2 = time-only format
function fmtKind(id, code) {
  if ((id >= 14 && id <= 17) || id === 22 || (id >= 27 && id <= 31) || (id >= 34 && id <= 36) || (id >= 50 && id <= 58) || (id >= 71 && id <= 81)) return 1;
  if ((id >= 18 && id <= 21) || (id >= 45 && id <= 47) || id === 32 || id === 33) return 2;
  if (!code) return 0;
  let c = code.replace(/"[^"]*"/g, '').replace(/\\./g, '').replace(/\[(?!h\]|hh\]|m\]|mm\]|s\]|ss\])[^\]]*\]/gi, '');
  c = c.split(';')[0];
  if (/[yd]/i.test(c) || /m{3,}/i.test(c)) return 1;
  if (/[hs]/i.test(c) || /\[m+\]/i.test(c)) return 2;
  return 0;
}

async function loadSharedStrings(file, entries) {
  const ent = entries['xl/sharedStrings.xml'];
  const sst = [];
  if (!ent) return sst;
  const s = await entryStream(file, ent);
  await scanSegments(s, '</si>', seg => {
    const re = /<si>([\s\S]*?)<\/si>|<si\/>/g;
    let m;
    while ((m = re.exec(seg))) sst.push(m[1] ? textOfRich(m[1]) : '');
  });
  return sst;
}

/* ---------------- sheet row iteration ---------------- */
// Cell kinds: 's' string, 'n' number, 'b' bool, 'e' error. Calls onRow(rowNumber, cells[]) where cells[i] = {k, v, st} or undefined.
async function iterateRows(file, wb, sheetPath, sst, onRow, onProgress) {
  const ent = wb.entries[sheetPath];
  if (!ent) throw new Error('Sheet not found in file: ' + sheetPath);
  let bytes = 0, lastReport = 0;
  const s = await entryStream(file, ent, n => {
    bytes += n;
    if (onProgress && bytes - lastReport > 1 << 20) { lastReport = bytes; onProgress(bytes / ent.compSize); }
  });
  const rowRe = /<row\b([^>]*?)(?:\/>|>([\s\S]*?)<\/row>)/g;
  const cellRe = /<c\b([^>]*?)(?:\/>|>([\s\S]*?)<\/c>)/g;
  let autoRow = 0, stopped = false;
  await scanSegments(s, '</row>', seg => {
    if (stopped) return true;
    rowRe.lastIndex = 0;
    let m;
    while ((m = rowRe.exec(seg))) {
      const ra = attr(m[1], 'r');
      const rn = ra ? parseInt(ra, 10) : autoRow + 1;
      autoRow = rn;
      const body = m[2];
      const cells = [];
      if (body) {
        cellRe.lastIndex = 0;
        let c, auto = 0;
        while ((c = cellRe.exec(body))) {
          const a = c[1];
          const r = attr(a, 'r');
          const ci = r ? colIndex(r) : auto;
          auto = ci + 1;
          const inner = c[2];
          if (!inner) continue;
          const t = attr(a, 't');
          const st = attr(a, 's');
          let cell;
          if (t === 'inlineStr') {
            cell = { k: 's', v: textOfRich(inner) };
          } else {
            const vm = /<v>([\s\S]*?)<\/v>/.exec(inner);
            if (!vm) continue;
            const raw = vm[1];
            if (t === 's') { const i = +raw; cell = { k: 's', v: sst[i] === undefined ? '' : sst[i], si: i }; }
            else if (t === 'str') cell = { k: 's', v: unescapeXml(raw) };
            else if (t === 'b') cell = { k: 'b', v: raw === '1' };
            else if (t === 'e') cell = { k: 'e', v: unescapeXml(raw) };
            else if (t === 'd') cell = { k: 's', v: raw };
            else { const num = parseFloat(raw); if (isNaN(num)) continue; cell = { k: 'n', v: num, df: st ? (wb.dateStyles[+st] || 0) : 0 }; }
          }
          cells[ci] = cell;
        }
      }
      if (onRow(rn, cells) === false) { stopped = true; return true; }
    }
  });
  if (onProgress) onProgress(1);
}

/* ---------------- value classification ---------------- */
const MONTHS = { jan: 1, feb: 2, mar: 3, apr: 4, may: 5, mei: 5, jun: 6, jul: 7, aug: 8, agu: 8, agt: 8, sep: 9, oct: 10, okt: 10, nov: 11, dec: 12, des: 12 };
const RE_YMD = /^(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})(?:[ T](\d{1,2}):(\d{2})(?::(\d{2})(?:\.\d+)?)?)?$/;
const RE_DMY = /^(\d{1,2})[-/.](\d{1,2})[-/.](\d{4})(?:[ T](\d{1,2}):(\d{2})(?::(\d{2})(?:\.\d+)?)?)?$/;
const RE_DMONY = /^(\d{1,2})[- ]([A-Za-z]{3})[a-z]*[- ,]+(\d{4}|\d{2})$/;
const RE_TIME = /^(\d{1,2}):(\d{2})(?::(\d{2})(?:\.\d+)?)?(?:\s?([AaPp])\.?[Mm]\.?)?$/;
const RE_NUMLIKE = /^[-+]?\*?[\d.,]*\d[\d.,]*-?$/;
const RE_US = /^(\d{1,3}(,\d{3})+|\d+)(\.\d+)?$/;
const RE_EU = /^(\d{1,3}(\.\d{3})+|\d+)(,\d+)?$/;

// classify a cleaned, trimmed, non-empty string
function classifyString(s) {
  let m;
  if (/^\d+$/.test(s)) return { c: 'digits', lead0: s.length > 1 && s[0] === '0', len: s.length };
  if ((m = RE_TIME.exec(s)) && +m[1] < 24 + (m[4] ? -11 : 0) + (m[4] ? 0 : 0) && +m[2] < 60) return { c: 'time' };
  if ((m = RE_YMD.exec(s))) { if (+m[2] >= 1 && +m[2] <= 12 && +m[3] >= 1 && +m[3] <= 31) return { c: 'dtext', ord: 'ymd', a: +m[2], b: +m[3], hasTime: !!m[4] }; }
  if ((m = RE_DMY.exec(s))) { const a = +m[1], b = +m[2]; if (a >= 1 && b >= 1 && a <= 31 && b <= 31 && (a <= 12 || b <= 12)) return { c: 'dtext', ord: 'xy', a, b, hasTime: !!m[4] }; }
  if ((m = RE_DMONY.exec(s)) && MONTHS[m[2].toLowerCase()]) return { c: 'dtext', ord: 'dmon', hasTime: false };
  if (/^(true|false)$/i.test(s)) return { c: 'bool' };
  if (RE_NUMLIKE.test(s)) {
    let body = s.replace(/^[-+]/, '').replace(/-$/, '').replace('*', '');
    const star = s.indexOf('*') >= 0, trailMinus = /\d-$/.test(s) && !/^[-+]/.test(s);
    if (/^[-+]/.test(s) && /-$/.test(s)) return { c: 'other' };
    const us = RE_US.test(body), eu = RE_EU.test(body);
    if (us || eu) {
      const dec = us ? ((body.split('.')[1] || '').length) : ((body.split(',')[1] || '').length);
      return { c: 'numtext', us, eu, star, trailMinus, decUS: (body.split('.')[1] || '').length, decEU: (body.split(',')[1] || '').length, dec };
    }
  }
  return { c: 'other' };
}

function cleanString(v, opts) {
  if (opts.removeLineBreaks && (v.indexOf('\n') >= 0 || v.indexOf('\r') >= 0)) v = v.replace(/[\r\n]+/g, ' ');
  if (v.indexOf('\t') >= 0) v = v.replace(/\t/g, ' ');
  if (opts.trim) v = v.trim();
  // strip control chars BigQuery rejects in CSV
  if (/[\x00-\x08\x0B\x0C\x0E-\x1F]/.test(v)) v = v.replace(/[\x00-\x08\x0B\x0C\x0E-\x1F]/g, '');
  return v;
}

/* ---------------- header sanitising ---------------- */
const RESERVED_PREFIX = /^(_TABLE_|_FILE_|_PARTITION|_ROW_TIMESTAMP|__ROOT__|_COLIDENTIFIER)/i;
function sanitizeHeaders(raw) {
  const used = new Set();
  const out = [];
  raw.forEach((h, i) => {
    let n = String(h == null ? '' : h).trim();
    n = n.normalize('NFKD').replace(/[̀-ͯ]/g, '');
    n = n.replace(/[^A-Za-z0-9_]+/g, '_').replace(/_+/g, '_').replace(/^_+|_+$/g, '');
    if (!n) n = 'column_' + (i + 1);
    if (/^\d/.test(n)) n = 'c_' + n;
    if (RESERVED_PREFIX.test(n)) n = 'c' + n;
    n = n.slice(0, 290);
    let cand = n, k = 2;
    while (used.has(cand.toLowerCase())) cand = n + '_' + (k++);
    used.add(cand.toLowerCase());
    out.push(cand);
  });
  return out;
}

/* ---------------- column profiling ---------------- */
function newStats() {
  return {
    nonEmpty: 0, errors: 0, num: 0, numInt: 0, numMaxDec: 0, numMaxAbs: 0,
    dInt: 0, dTime: 0, dDT: 0, dtext: 0, dtextTime: 0, dtextYMD: 0, dtextXY: 0, dtextDmon: 0, maxA: 0, maxB: 0,
    ttext: 0, numtext: 0, ntUS: 0, ntEU: 0, ntDecUS: 0, ntDecEU: 0, star: 0, trailMinus: 0,
    digits: 0, digitsLead0: 0, digitsMaxLen: 0, bool: 0, other: 0, newlines: 0, maxLen: 0, examples: [], otherExamples: []
  };
}
function addExample(arr, v, max) { if (arr.length < max && arr.indexOf(v) < 0) arr.push(v); }

function profileCell(st, cell, opts, cache) {
  if (!cell) return;
  if (cell.k === 'e') { st.errors++; return; }
  if (cell.k === 'b') { st.nonEmpty++; st.bool++; return; }
  if (cell.k === 'n') {
    st.nonEmpty++;
    const v = cell.v;
    if (cell.df) {
      if (cell.df === 2 || (v >= 0 && v < 1)) st.dTime++;
      else if (Number.isInteger(v)) st.dInt++;
      else st.dDT++;
    } else {
      st.num++;
      if (Number.isInteger(v)) st.numInt++;
      else { const d = decimals(v); if (d > st.numMaxDec) st.numMaxDec = d; }
      const a = Math.abs(v); if (a > st.numMaxAbs) st.numMaxAbs = a;
    }
    if (st.examples.length < 3) addExample(st.examples, String(v), 3);
    return;
  }
  // string
  const raw = cell.v;
  if (raw.indexOf('\n') >= 0 || raw.indexOf('\r') >= 0) st.newlines++;
  let cls;
  if (cell.si !== undefined && cache) {
    cls = cache[cell.si];
    if (cls === undefined) { const v = cleanString(raw, opts); cls = cache[cell.si] = v === '' ? null : Object.assign(classifyString(v), { len: v.length, val: v }); }
  } else {
    const v = cleanString(raw, opts);
    cls = v === '' ? null : Object.assign(classifyString(v), { len: v.length, val: v });
  }
  if (!cls) return;
  st.nonEmpty++;
  if (cls.len > st.maxLen) st.maxLen = cls.len;
  if (st.examples.length < 3) addExample(st.examples, cls.val.slice(0, 60), 3);
  switch (cls.c) {
    case 'digits': st.digits++; if (cls.lead0) st.digitsLead0++; if (cls.len > st.digitsMaxLen) st.digitsMaxLen = cls.len; break;
    case 'time': st.ttext++; break;
    case 'dtext':
      st.dtext++; if (cls.hasTime) st.dtextTime++;
      if (cls.ord === 'ymd') st.dtextYMD++; else if (cls.ord === 'dmon') st.dtextDmon++;
      else { st.dtextXY++; if (cls.a > st.maxA) st.maxA = cls.a; if (cls.b > st.maxB) st.maxB = cls.b; }
      break;
    case 'numtext':
      st.numtext++; if (cls.us) st.ntUS++; if (cls.eu) st.ntEU++;
      if (cls.decUS > st.ntDecUS) st.ntDecUS = cls.decUS; if (cls.decEU > st.ntDecEU) st.ntDecEU = cls.decEU;
      if (cls.star) st.star++; if (cls.trailMinus) st.trailMinus++;
      break;
    case 'bool': st.bool++; break;
    default: st.other++; addExample(st.otherExamples, cls.val.slice(0, 60), 3);
  }
}
function decimals(v) {
  let s = String(parseFloat(v.toPrecision(15)));
  if (s.indexOf('e') >= 0) return 10;
  const i = s.indexOf('.');
  return i < 0 ? 0 : s.length - i - 1;
}

// decide BigQuery type + parsing hints from stats
function decideType(st, opts, name) {
  const N = st.nonEmpty;
  const notes = [];
  const info = { type: 'STRING', notes, numFormat: 'US', dateOrder: 'DMY' };
  if (st.errors) notes.push(st.errors + ' Excel error cell(s) (#N/A etc.) will be left empty');
  if (st.newlines && opts.removeLineBreaks) notes.push(st.newlines + ' cell(s) had line breaks → replaced with a space');
  if (N === 0) { notes.push('Column is completely empty → loaded as STRING (all NULL)'); return info; }
  // date order for xy texts
  if (st.dtextXY) {
    if (opts.dateOrder === 'DMY' || opts.dateOrder === 'MDY') info.dateOrder = opts.dateOrder;
    else if (st.maxA > 12 && st.maxB <= 12) info.dateOrder = 'DMY';
    else if (st.maxB > 12 && st.maxA <= 12) info.dateOrder = 'MDY';
    else { info.dateOrder = 'DMY'; if (st.maxA <= 12 && st.maxB <= 12) notes.push('Day/month order is ambiguous in this column → read as DD/MM/YYYY (change "Text date order" if wrong)'); }
  }
  if (st.bool === N) { info.type = 'BOOL'; return info; }
  const dateish = st.dInt + st.dDT + st.dtext;
  if (dateish > 0 && dateish === N) {
    info.type = (st.dDT + st.dtextTime) > 0 ? 'DATETIME' : 'DATE';
    if (st.dtext) notes.push(st.dtext + ' text date(s) converted to ' + (info.type === 'DATE' ? 'YYYY-MM-DD' : 'YYYY-MM-DD HH:MM:SS'));
    return info;
  }
  if (st.dTime + st.ttext === N) { info.type = 'TIME'; return info; }
  // numeric?
  const realNum = st.num + st.numtext;
  let nf = 'US';
  if (st.numtext) {
    if (opts.numberFormat === 'US' || opts.numberFormat === 'EU') nf = opts.numberFormat;
    else if (st.ntUS === st.numtext) nf = 'US';
    else if (st.ntEU === st.numtext) nf = 'EU';
    else nf = null;
    if (nf === 'US' && st.ntUS !== st.numtext) nf = null;
    if (nf === 'EU' && st.ntEU !== st.numtext) nf = null;
  }
  const digitsOK = st.digits === 0 || (realNum > 0 && st.digitsLead0 === 0 && st.digitsMaxLen <= 15);
  if (realNum > 0 && nf && digitsOK && realNum + st.digits === N) {
    info.numFormat = nf;
    const ntDec = nf === 'US' ? st.ntDecUS : st.ntDecEU;
    const maxDec = Math.max(st.numMaxDec, ntDec);
    const allInt = st.numInt === st.num && ntDec === 0;
    if (allInt && st.numMaxAbs < 9.2e18) info.type = 'INT64';
    else if (maxDec <= 9 && st.numMaxAbs < 1e28) info.type = 'NUMERIC';
    else info.type = 'FLOAT64';
    if (st.numtext) notes.push(st.numtext + ' number(s) stored as text' + (nf === 'US' ? ' (1,234.56 style)' : ' (1.234,56 style)') + ' → converted to plain numbers');
    if (st.star) notes.push(st.star + ' value(s) had a leading * (SAP overflow marker) → removed; check these values');
    if (st.trailMinus) notes.push(st.trailMinus + ' value(s) used SAP trailing minus (e.g. 5-) → converted to -5');
    return info;
  }
  // STRING
  if (st.digits && st.digits === N) notes.push('Codes/IDs stored as text' + (st.digitsLead0 ? ' (some with leading zeros)' : '') + ' → kept as STRING');
  else if (realNum && st.other) notes.push('Mixed numbers and text (e.g. "' + st.otherExamples.join('", "') + '") → STRING');
  else if (dateish && dateish < N) notes.push('Mixed dates and other values → STRING');
  if (st.maxLen > 1024) notes.push('Long text (up to ' + st.maxLen + ' chars)');
  return info;
}

/* ---------------- formatting (pass 2) ---------------- */
const pad2 = n => (n < 10 ? '0' : '') + n;
function serialToParts(v, date1904) {
  let days = Math.floor(v);
  let secs = Math.round((v - days) * 86400);
  if (secs >= 86400) { days += 1; secs -= 86400; }
  let ms;
  if (date1904) ms = Date.UTC(1904, 0, 1) + days * 86400000;
  else if (days < 61) ms = Date.UTC(1899, 11, 31) + days * 86400000;
  else ms = Date.UTC(1899, 11, 30) + days * 86400000;
  const d = new Date(ms);
  return { y: d.getUTCFullYear(), m: d.getUTCMonth() + 1, d: d.getUTCDate(), hh: Math.floor(secs / 3600), mi: Math.floor(secs / 60) % 60, ss: secs % 60 };
}
const fmtDate = p => p.y + '-' + pad2(p.m) + '-' + pad2(p.d);
const fmtTime = p => pad2(p.hh) + ':' + pad2(p.mi) + ':' + pad2(p.ss);
function fmtNum(v, type) {
  if (type === 'NUMERIC') v = Number(v.toFixed(9));
  if (Number.isInteger(v)) return Math.abs(v) < 1e21 ? String(v) : BigInt(v).toString();
  let s = String(parseFloat(v.toPrecision(15)));
  if (s.indexOf('e') >= 0) s = v.toFixed(20).replace(/0+$/, '').replace(/\.$/, '');
  return s;
}
function parseNumText(s, nf) {
  s = s.trim();
  let neg = false;
  if (s[0] === '-') { neg = true; s = s.slice(1); } else if (s[0] === '+') s = s.slice(1);
  if (s[s.length - 1] === '-') { neg = !neg; s = s.slice(0, -1); }
  s = s.replace('*', '');
  if (nf === 'EU') { if (!RE_EU.test(s)) return null; s = s.replace(/\./g, '').replace(',', '.'); }
  else { if (!RE_US.test(s)) return null; s = s.replace(/,/g, ''); }
  const n = parseFloat(s);
  return isNaN(n) ? null : (neg ? -n : n);
}
function parseDateText(s, order) {
  let m;
  if ((m = RE_YMD.exec(s))) return { y: +m[1], m: +m[2], d: +m[3], hh: +(m[4] || 0), mi: +(m[5] || 0), ss: +(m[6] || 0) };
  if ((m = RE_DMY.exec(s))) {
    const a = +m[1], b = +m[2];
    const [d, mo] = order === 'MDY' ? [b, a] : [a, b];
    if (mo < 1 || mo > 12 || d < 1 || d > 31) return null;
    return { y: +m[3], m: mo, d, hh: +(m[4] || 0), mi: +(m[5] || 0), ss: +(m[6] || 0) };
  }
  if ((m = RE_DMONY.exec(s))) {
    const mo = MONTHS[m[2].toLowerCase()]; if (!mo) return null;
    let y = +m[3]; if (m[3].length === 2) y += y < 70 ? 2000 : 1900;
    return { y, m: mo, d: +m[1], hh: 0, mi: 0, ss: 0 };
  }
  return null;
}
function parseTimeText(s) {
  const m = RE_TIME.exec(s);
  if (!m) { const dm = RE_YMD.exec(s) || RE_DMY.exec(s); return dm && dm[4] ? parseDateText(s, 'DMY') : null; }
  let hh = +m[1];
  if (m[4]) { const pm = /p/i.test(m[4]); if (hh === 12) hh = 0; if (pm) hh += 12; }
  if (hh > 23 || +m[2] > 59) return null;
  return { hh, mi: +m[2], ss: +(m[3] || 0) };
}
function validDate(p) { return p && p.m >= 1 && p.m <= 12 && p.d >= 1 && p.d <= 31 && p.y >= 1 && p.y <= 9999; }

// returns string for CSV, or null when the value can't be converted to the column type
function formatCell(cell, col, opts, date1904) {
  if (!cell || cell.k === 'e') return '';
  const T = col.type;
  if (cell.k === 'b') return T === 'BOOL' || T === 'STRING' ? (cell.v ? 'true' : 'false') : (T === 'INT64' || T === 'NUMERIC' || T === 'FLOAT64' ? (cell.v ? '1' : '0') : null);
  if (cell.k === 'n') {
    const v = cell.v;
    switch (T) {
      case 'DATE': return fmtDate(serialToParts(v, date1904));
      case 'DATETIME': { const p = serialToParts(v, date1904); return fmtDate(p) + ' ' + fmtTime(p); }
      case 'TIME': { const p = serialToParts(v - Math.floor(v), date1904); if (p.hh === 0 && v - Math.floor(v) > 0.5) { p.hh = 23; p.mi = 59; p.ss = 59; } return fmtTime(p); }
      case 'INT64': return Number.isInteger(v) ? fmtNum(v, T) : null;
      case 'BOOL': return v === 1 ? 'true' : v === 0 ? 'false' : null;
      case 'STRING':
        if (cell.df) { const p = serialToParts(v, date1904); if (cell.df === 2 || (v >= 0 && v < 1)) return fmtTime(p); return Number.isInteger(v) ? fmtDate(p) : fmtDate(p) + ' ' + fmtTime(p); }
        return fmtNum(v, T);
      default: return fmtNum(v, T);
    }
  }
  // string
  const s = cleanString(cell.v, opts);
  if (s === '') return '';
  switch (T) {
    case 'STRING': return s;
    case 'INT64': case 'NUMERIC': case 'FLOAT64': {
      const n = parseNumText(s, col.numFormat || 'US');
      if (n === null) return null;
      if (T === 'INT64' && !Number.isInteger(n)) return null;
      return fmtNum(n, T);
    }
    case 'DATE': { const p = parseDateText(s, col.dateOrder); return validDate(p) ? fmtDate(p) : null; }
    case 'DATETIME': { const p = parseDateText(s, col.dateOrder); return validDate(p) ? fmtDate(p) + ' ' + fmtTime(p) : null; }
    case 'TIME': { const p = parseTimeText(s); return p ? fmtTime(p) : null; }
    case 'BOOL': { if (/^(true|1|yes|y|x)$/i.test(s)) return 'true'; if (/^(false|0|no|n)$/i.test(s)) return 'false'; return null; }
  }
  return s;
}
function csvField(s) {
  if (s === '') return '';
  if (/[",\r\n]/.test(s)) return '"' + s.replace(/"/g, '""') + '"';
  return s;
}

/* ---------------- passes ---------------- */
function headerValue(cell, date1904) {
  if (!cell) return '';
  if (cell.k === 'n') return fmtNum(cell.v);
  if (cell.k === 'b') return cell.v ? 'TRUE' : 'FALSE';
  return String(cell.v);
}

async function analyse(file, sheetPath, opts, progress) {
  const wb = await openWorkbook(file);
  progress('Reading text values…', 0);
  const sst = await loadSharedStrings(file, wb.entries);
  const cache = new Array(sst.length);
  let headerRaw = null, stats = [], rows = 0, blankRows = 0, sample = [], maxCols = 0;
  await iterateRows(file, wb, sheetPath, sst, (rn, cells) => {
    if (rn < opts.headerRow) return;
    if (rn === opts.headerRow || headerRaw === null) {
      headerRaw = [];
      for (let i = 0; i < cells.length; i++) headerRaw[i] = headerValue(cells[i], wb.date1904);
      // trim trailing empty header cells
      while (headerRaw.length && headerRaw[headerRaw.length - 1] === '') headerRaw.pop();
      return;
    }
    let any = false;
    for (let i = 0; i < cells.length; i++) { const c = cells[i]; if (c && !(c.k === 's' && c.v.trim() === '')) { any = true; break; } }
    if (!any) { blankRows++; return; }
    rows++;
    if (cells.length > maxCols) maxCols = cells.length;
    for (let i = 0; i < cells.length; i++) {
      if (!cells[i]) continue;
      if (!stats[i]) stats[i] = newStats();
      profileCell(stats[i], cells[i], opts, cache);
    }
    if (sample.length < 8) sample.push(cells);
  }, f => progress('Analysing rows…', f));
  if (!headerRaw) throw new Error('No header row found (the sheet looks empty, or the header row number is too high).');
  const nCols = Math.max(headerRaw.length, maxCols);
  // drop trailing columns that are totally empty and have no header
  let last = nCols - 1;
  while (last >= 0 && !headerRaw[last] && !(stats[last] && (stats[last].nonEmpty || stats[last].errors))) last--;
  const raw = [];
  for (let i = 0; i <= last; i++) raw.push(headerRaw[i] || '');
  const names = sanitizeHeaders(raw);
  const lowerCount = {};
  raw.forEach(h => { const k = String(h).trim().toLowerCase(); lowerCount[k] = (lowerCount[k] || 0) + 1; });
  const columns = raw.map((h, i) => {
    const st = stats[i] || newStats();
    const info = decideType(st, opts, names[i]);
    const hn = [];
    if (!String(h).trim()) hn.push('Header was empty → named ' + names[i]);
    else if (lowerCount[String(h).trim().toLowerCase()] > 1) hn.push('Header "' + h + '" appears ' + lowerCount[String(h).trim().toLowerCase()] + ' times → this one is named ' + names[i]);
    else if (names[i] !== String(h)) hn.push('Header "' + h + '" has characters BigQuery does not allow → renamed ' + names[i]);
    return { index: i, original: String(h), name: names[i], type: info.type, detected: info.type, numFormat: info.numFormat, dateOrder: info.dateOrder, notes: hn.concat(info.notes), nonEmpty: st.nonEmpty, examples: st.examples, maxLen: st.maxLen };
  });
  return { rows, blankRows, columns, sample, date1904: wb.date1904 };
}

function previewRows(sample, columns, opts, date1904) {
  return sample.map(cells => columns.map(col => { const v = formatCell(cells[col.index], col, opts, date1904); return v === null ? '⚠ ' + rawText(cells[col.index]) : v; }));
}
function rawText(cell) { return !cell ? '' : String(cell.v); }

async function build(file, sheetPath, opts, columns, progress) {
  const wb = await openWorkbook(file);
  progress('Reading text values…', 0);
  const sst = await loadSharedStrings(file, wb.entries);
  const parts = [];
  let buf = [];
  let bufLen = 0;
  const flush = () => { if (buf.length) { parts.push(buf.join('')); buf = []; bufLen = 0; } };
  const header = columns.map(c => csvField(c.name)).join(',') + '\n';
  buf.push(header);
  const fails = columns.map(() => 0);
  const failEx = columns.map(() => []);
  let rows = 0, seenHeader = false;
  await iterateRows(file, wb, sheetPath, sst, (rn, cells) => {
    if (rn < opts.headerRow) return;
    if (!seenHeader) { seenHeader = true; return; }
    let any = false;
    for (let i = 0; i < cells.length; i++) { const c = cells[i]; if (c && !(c.k === 's' && c.v.trim() === '')) { any = true; break; } }
    if (!any) return;
    rows++;
    let line = '';
    for (let j = 0; j < columns.length; j++) {
      const col = columns[j];
      let v = formatCell(cells[col.index], col, opts, wb.date1904);
      if (v === null) { fails[j]++; if (failEx[j].length < 3) failEx[j].push(rawText(cells[col.index]).slice(0, 40)); v = ''; }
      line += (j ? ',' : '') + csvField(v);
    }
    buf.push(line + '\n');
    bufLen += line.length;
    if (bufLen > 4e6) flush();
  }, f => progress('Writing CSV…', f));
  flush();
  progress('Compressing (gzip)…', 1);
  const csv = new Blob(parts, { type: 'text/csv' });
  const gz = await new Response(csv.stream().pipeThrough(new CompressionStream('gzip'))).blob();
  const failures = columns.map((c, j) => fails[j] ? { name: c.name, type: c.type, count: fails[j], examples: failEx[j] } : null).filter(Boolean);
  return { csv, gz, rows, failures };
}

/* ---------------- worker wiring ---------------- */
if (typeof self !== 'undefined' && typeof WorkerGlobalScope !== 'undefined' && self instanceof WorkerGlobalScope) {
  let lastAnalysis = null;
  self.onmessage = async ev => {
    const msg = ev.data;
    const progress = (label, frac) => self.postMessage({ type: 'progress', label, frac });
    try {
      if (msg.cmd === 'open') {
        const wb = await openWorkbook(msg.file);
        self.postMessage({ type: 'opened', sheets: wb.sheets });
      } else if (msg.cmd === 'analyse') {
        const res = await analyse(msg.file, msg.sheetPath, msg.opts, progress);
        lastAnalysis = res;
        self.postMessage({ type: 'analysed', rows: res.rows, blankRows: res.blankRows, columns: res.columns, preview: previewRows(res.sample, res.columns, msg.opts, res.date1904) });
      } else if (msg.cmd === 'preview') {
        if (lastAnalysis) self.postMessage({ type: 'preview', preview: previewRows(lastAnalysis.sample, msg.columns, msg.opts, lastAnalysis.date1904) });
      } else if (msg.cmd === 'build') {
        const res = await build(msg.file, msg.sheetPath, msg.opts, msg.columns, progress);
        self.postMessage({ type: 'built', csv: res.csv, gz: res.gz, rows: res.rows, failures: res.failures });
      }
    } catch (e) {
      self.postMessage({ type: 'error', message: (e && e.message) || String(e) });
    }
  };
}
if (typeof module !== 'undefined') module.exports = { openWorkbook, analyse, build, previewRows, sanitizeHeaders, classifyString, formatCell };
