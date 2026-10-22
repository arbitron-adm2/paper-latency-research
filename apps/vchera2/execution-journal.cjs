'use strict';
const { recoverClosing } = require('./execution-state.cjs');
class ExecutionJournal {
  constructor(db) {
    this.db = db;
    for (const name of ['close_state', 'close_evidence']) {
      const columns = db.prepare('PRAGMA table_info(btcy2_lag_trades)').all().map(c => c.name);
      if (!columns.includes(name)) db.exec(`ALTER TABLE btcy2_lag_trades ADD COLUMN ${name} TEXT`);
    }
    db.exec('CREATE TABLE IF NOT EXISTS btcy2_execution_audit (id INTEGER PRIMARY KEY, at INTEGER NOT NULL, position_id TEXT, action TEXT NOT NULL, original_row TEXT NOT NULL)');
  }
  ensureUnique() {
    this.db.exec("CREATE UNIQUE INDEX IF NOT EXISTS btcy2_live_position_once ON btcy2_lag_trades(position_id) WHERE live=1 AND position_id IS NOT NULL AND status != 'duplicate'");
  }
  find(positionId) {
    return this.db.prepare("SELECT * FROM btcy2_lag_trades WHERE live=1 AND position_id=? AND status != 'duplicate' ORDER BY id LIMIT 1").get(String(positionId));
  }
  saveClosing(p) {
    if (!p.live || !p.closing) return;
    const value = { ...p.closing }; delete value.sending;
    this.db.prepare('UPDATE btcy2_lag_trades SET close_state=? WHERE id=?').run(JSON.stringify(value), p.id);
  }
  closing(row) { return recoverClosing(row); }
  evidence(id, evidence) {
    this.db.prepare('UPDATE btcy2_lag_trades SET close_evidence=? WHERE id=?').run(JSON.stringify(evidence), id);
  }
  // Only repair duplicates with confirmed exchange history. Preserve every original row in audit.
  repairDuplicates(confirmed, reasons = {}) {
    const groups = this.db.prepare("SELECT position_id FROM btcy2_lag_trades WHERE live=1 AND position_id IS NOT NULL AND status!='duplicate' GROUP BY position_id HAVING COUNT(*)>1").all();
    const result = [];
    this.db.exec('BEGIN IMMEDIATE');
    try {
      for (const { position_id: pid } of groups) {
        const ev = confirmed.get(String(pid));
        if (!ev || !Number.isFinite(ev.px) || !Number.isFinite(ev.ts) || !Number.isFinite(ev.realPnl)) throw new Error('Confirmed close required for duplicate repair: ' + pid);
        const rows = this.db.prepare("SELECT * FROM btcy2_lag_trades WHERE live=1 AND position_id=? AND status!='duplicate' ORDER BY id").all(pid);
        const first = rows[0];
        for (const row of rows) this.db.prepare('INSERT INTO btcy2_execution_audit(at,position_id,action,original_row) VALUES(?,?,?,?)').run(Date.now(), pid, 'deduplicate', JSON.stringify(row));
        const exits = rows.map(r => { try { return JSON.parse(r.exit_info || 'null'); } catch { return null; } }).filter(x => x && x.decidedAt <= ev.ts);
        exits.sort((a,b) => (a.decidedAt - b.decidedAt) || Number(!!b.sentAt) - Number(!!a.sentAt));
        const exit = exits[0] ? { ...exits[0], filledAt: ev.ts, exit: ev.px } : null;
        const reason = reasons[pid] || 'подтверждённое закрытие; исходная причина неизвестна';
        const entry = rows.find(r => r.entry_info)?.entry_info || null;
        const gross = (first.side === 'L' ? 1 : -1) * (ev.px - first.entry) * first.qty;
        this.db.prepare("UPDATE btcy2_lag_trades SET status='closed',closed_at=?,exit=?,reason=?,gross_pnl=?,est_pnl=?,real_pnl=?,entry_info=?,exit_info=?,close_evidence=? WHERE id=?")
          .run(ev.ts, ev.px, reason, Math.round(gross*100)/100, ev.realPnl, ev.realPnl, entry, exit ? JSON.stringify(exit) : null, JSON.stringify(ev), first.id);
        for (const row of rows.slice(1)) this.db.prepare("UPDATE btcy2_lag_trades SET status='duplicate' WHERE id=?").run(row.id);
        result.push({ positionId: pid, canonicalId: first.id, duplicateIds: rows.slice(1).map(r=>r.id), reason });
      }
      this.ensureUnique();
      this.db.exec('COMMIT');
      return result;
    } catch (e) { this.db.exec('ROLLBACK'); throw e; }
  }
}
module.exports = { ExecutionJournal };
