import { useEffect, useState } from "react";
import { Alert, Button, Card, Input, Select, Space, Switch, Table, Typography, Upload } from "antd";

const API = "http://127.0.0.1:8000";
const ENTITY_OPTIONS = [
  "courses",
  "programs",
  "requirements",
  "instructors",
  "classrooms",
  "sections",
  "cadets",
  "records",
  "prerequisites",
  "substitutions"
];

async function uploadCsv(entity, file, mode) {
  const token = localStorage.getItem("session_token") || "";
  const form = new FormData();
  form.append("file", file);
  const suffix = mode === "validate" ? "/validate" : "";
  const url = `${API}/import/csv/${entity}${suffix}?session_token=${encodeURIComponent(token)}`;
  const r = await fetch(url, { method: "POST", body: form });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export function ImportPage() {
  const [entity, setEntity] = useState("courses");
  const [file, setFile] = useState(null);
  const [coiFile, setCoiFile] = useState(null);
  const [versionId, setVersionId] = useState("");
  const [replaceExisting, setReplaceExisting] = useState("NO");
  const [minOccurrences, setMinOccurrences] = useState(2);
  const [minConfidence, setMinConfidence] = useState(0.6);
  const [coiAnalyze, setCoiAnalyze] = useState(null);
  const [coiLoad, setCoiLoad] = useState(null);
  const [reviewSession, setReviewSession] = useState(null);
  const [reviewItems, setReviewItems] = useState([]);
  const [reviewCounts, setReviewCounts] = useState(null);
  const [versions, setVersions] = useState([]);
  const [validateResult, setValidateResult] = useState(null);
  const [commitResult, setCommitResult] = useState(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  async function loadVersions() {
    const token = localStorage.getItem("session_token") || "";
    const r = await fetch(`${API}/versions?session_token=${encodeURIComponent(token)}`);
    if (!r.ok) return;
    const data = await r.json();
    setVersions(data || []);
    if (!versionId && data?.length) {
      const active = data.find((v) => v.status === "ACTIVE");
      setVersionId((active || data[0]).id);
    }
  }

  useEffect(() => {
    loadVersions();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function runValidate() {
    if (!file) return;
    setBusy(true);
    setError("");
    setCommitResult(null);
    try {
      const result = await uploadCsv(entity, file, "validate");
      setValidateResult(result);
    } catch (e) {
      setError(String(e.message || e));
    } finally {
      setBusy(false);
    }
  }

  async function runCommit() {
    if (!file) return;
    setBusy(true);
    setError("");
    try {
      const result = await uploadCsv(entity, file, "commit");
      setCommitResult(result);
    } catch (e) {
      setError(String(e.message || e));
    } finally {
      setBusy(false);
    }
  }

  async function runCoiAnalyze() {
    if (!coiFile || !versionId) return;
    setBusy(true);
    setError("");
    try {
      const token = localStorage.getItem("session_token") || "";
      const form = new FormData();
      form.append("file", coiFile);
      const url = `${API}/import/coi/analyze?version_id=${encodeURIComponent(versionId)}&replace_existing=${replaceExisting === "YES" ? "true" : "false"}&default_credit_hours=3.0&min_course_number_occurrences=${encodeURIComponent(minOccurrences)}&min_confidence=${encodeURIComponent(minConfidence)}&session_token=${encodeURIComponent(token)}`;
      const r = await fetch(url, { method: "POST", body: form });
      if (!r.ok) throw new Error(await r.text());
      setCoiAnalyze(await r.json());
      setCoiLoad(null);
    } catch (e) {
      setError(String(e.message || e));
    } finally {
      setBusy(false);
    }
  }

  async function runCoiLoad() {
    if (!coiFile || !versionId) return;
    setBusy(true);
    setError("");
    try {
      const token = localStorage.getItem("session_token") || "";
      const form = new FormData();
      form.append("file", coiFile);
      const url = `${API}/import/coi/load-baseline?version_id=${encodeURIComponent(versionId)}&replace_existing=${replaceExisting === "YES" ? "true" : "false"}&default_credit_hours=3.0&min_course_number_occurrences=${encodeURIComponent(minOccurrences)}&min_confidence=${encodeURIComponent(minConfidence)}&session_token=${encodeURIComponent(token)}`;
      const r = await fetch(url, { method: "POST", body: form });
      if (!r.ok) throw new Error(await r.text());
      setCoiLoad(await r.json());
    } catch (e) {
      setError(String(e.message || e));
    } finally {
      setBusy(false);
    }
  }

  async function runCoiReviewStart() {
    if (!coiFile || !versionId) return;
    setBusy(true);
    setError("");
    try {
      const token = localStorage.getItem("session_token") || "";
      const form = new FormData();
      form.append("file", coiFile);
      const url = `${API}/import/coi/review/start?version_id=${encodeURIComponent(versionId)}&replace_existing=${replaceExisting === "YES" ? "true" : "false"}&default_credit_hours=3.0&min_course_number_occurrences=${encodeURIComponent(minOccurrences)}&min_confidence=${encodeURIComponent(minConfidence)}&session_token=${encodeURIComponent(token)}`;
      const r = await fetch(url, { method: "POST", body: form });
      if (!r.ok) throw new Error(await r.text());
      const data = await r.json();
      setReviewSession(data);
      await loadReviewSession(data.session_id);
    } catch (e) {
      setError(String(e.message || e));
    } finally {
      setBusy(false);
    }
  }

  async function loadReviewSession(sessionId) {
    const token = localStorage.getItem("session_token") || "";
    const r = await fetch(`${API}/import/coi/review/${sessionId}?session_token=${encodeURIComponent(token)}`);
    if (!r.ok) {
      setError(await r.text());
      return;
    }
    const data = await r.json();
    setReviewItems(data.items || []);
    setReviewCounts(data.counts || null);
  }

  function patchReviewItem(itemId, patch) {
    setReviewItems((prev) => prev.map((x) => (x.id === itemId ? { ...x, ...patch } : x)));
  }

  async function saveReviewDecisions() {
    if (!reviewSession?.session_id) return;
    setBusy(true);
    setError("");
    try {
      const token = localStorage.getItem("session_token") || "";
      const body = {
        decisions: reviewItems.map((x) => ({
          item_id: x.id,
          include: !!x.include,
          edited_title: x.edited_title || x.title
        }))
      };
      const r = await fetch(`${API}/import/coi/review/${reviewSession.session_id}/decide?session_token=${encodeURIComponent(token)}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body)
      });
      if (!r.ok) throw new Error(await r.text());
      await loadReviewSession(reviewSession.session_id);
    } catch (e) {
      setError(String(e.message || e));
    } finally {
      setBusy(false);
    }
  }

  async function commitReviewSession() {
    if (!reviewSession?.session_id) return;
    setBusy(true);
    setError("");
    try {
      const token = localStorage.getItem("session_token") || "";
      const r = await fetch(`${API}/import/coi/review/${reviewSession.session_id}/commit?session_token=${encodeURIComponent(token)}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ replace_existing: replaceExisting === "YES", default_credit_hours: 3.0 })
      });
      if (!r.ok) throw new Error(await r.text());
      setCoiLoad(await r.json());
    } catch (e) {
      setError(String(e.message || e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <Space direction="vertical" style={{ width: "100%" }}>
      <Card title="Import Workbench (Phase 1)">
        <Space direction="vertical" style={{ width: "100%" }}>
          <Typography.Text>
            Validate CSV before commit, then execute import for selected entity.
          </Typography.Text>
          <Space wrap>
            <Select
              style={{ width: 260 }}
              value={entity}
              onChange={setEntity}
              options={ENTITY_OPTIONS.map((x) => ({ value: x, label: x }))}
            />
            <Upload
              beforeUpload={(f) => {
                setFile(f);
                return false;
              }}
              maxCount={1}
              accept=".csv,text/csv"
            >
              <Button>Select CSV</Button>
            </Upload>
            <Button type="default" onClick={runValidate} disabled={!file || busy}>
              Validate
            </Button>
            <Button type="primary" onClick={runCommit} disabled={!file || busy}>
              Commit Import
            </Button>
          </Space>
          {file && <Typography.Text type="secondary">Selected file: {file.name}</Typography.Text>}
          {error && <Alert type="error" message="Import Error" description={error} />}
        </Space>
      </Card>

      {validateResult && (
        <Card title="Validation Result">
          <pre>{JSON.stringify(validateResult, null, 2)}</pre>
          {(validateResult.errors || []).length > 0 && (
            <Table
              size="small"
              rowKey={(r, idx) => `${r.line}-${idx}`}
              dataSource={validateResult.errors || []}
              pagination={{ pageSize: 5 }}
              columns={[
                { title: "Line", dataIndex: "line", width: 80 },
                { title: "Error", dataIndex: "error" },
                { title: "Row", dataIndex: "row", render: (v) => <pre>{JSON.stringify(v, null, 2)}</pre> }
              ]}
            />
          )}
        </Card>
      )}

      {commitResult && (
        <Card title="Commit Result">
          <pre>{JSON.stringify(commitResult, null, 2)}</pre>
        </Card>
      )}

      <Card title="COI Baseline Loader">
        <Space direction="vertical" style={{ width: "100%" }}>
          <Typography.Text>
            Upload extracted COI text (`.txt`) to analyze and load baseline courses into a selected curriculum version.
          </Typography.Text>
          <Space wrap>
            <Select
              style={{ width: 360 }}
              placeholder="Curriculum version"
              value={versionId}
              onChange={setVersionId}
              options={versions.map((v) => ({ value: v.id, label: `${v.name} (${v.status})` }))}
            />
            <Select
              style={{ width: 180 }}
              value={replaceExisting}
              onChange={setReplaceExisting}
              options={[
                { value: "NO", label: "Keep Existing" },
                { value: "YES", label: "Replace Existing" }
              ]}
            />
            <Select
              style={{ width: 180 }}
              value={minOccurrences}
              onChange={setMinOccurrences}
              options={[
                { value: 1, label: "Min Occurrences 1" },
                { value: 2, label: "Min Occurrences 2" },
                { value: 3, label: "Min Occurrences 3" }
              ]}
            />
            <Select
              style={{ width: 180 }}
              value={minConfidence}
              onChange={setMinConfidence}
              options={[
                { value: 0.4, label: "Min Confidence 0.4" },
                { value: 0.6, label: "Min Confidence 0.6" },
                { value: 0.8, label: "Min Confidence 0.8" }
              ]}
            />
            <Upload
              beforeUpload={(f) => {
                setCoiFile(f);
                return false;
              }}
              maxCount={1}
              accept=".txt,text/plain"
            >
              <Button>Select COI Text File</Button>
            </Upload>
            <Button onClick={runCoiAnalyze} disabled={!coiFile || !versionId || busy}>
              Analyze COI
            </Button>
            <Button onClick={runCoiReviewStart} disabled={!coiFile || !versionId || busy}>
              Start Review Session
            </Button>
            <Button type="primary" onClick={runCoiLoad} disabled={!coiFile || !versionId || busy}>
              Load Baseline
            </Button>
          </Space>
          {coiFile && <Typography.Text type="secondary">Selected COI text: {coiFile.name}</Typography.Text>}
        </Space>
      </Card>

      {coiAnalyze && (
        <Card title="COI Analyze Result">
          <pre>{JSON.stringify(coiAnalyze, null, 2)}</pre>
        </Card>
      )}

      {reviewSession && (
        <Card title="COI Review Session">
          <Space direction="vertical" style={{ width: "100%" }}>
            <Typography.Text>Session: {reviewSession.session_id}</Typography.Text>
            <pre>{JSON.stringify(reviewCounts, null, 2)}</pre>
            <Space>
              <Button onClick={saveReviewDecisions} disabled={busy}>
                Save Decisions
              </Button>
              <Button type="primary" onClick={commitReviewSession} disabled={busy}>
                Commit Reviewed Baseline
              </Button>
            </Space>
            <Table
              size="small"
              rowKey="id"
              dataSource={reviewItems}
              pagination={{ pageSize: 8 }}
              columns={[
                { title: "Course", dataIndex: "course_number" },
                { title: "Confidence", dataIndex: "confidence", width: 110 },
                {
                  title: "Include",
                  dataIndex: "include",
                  width: 100,
                  render: (v, r) => <Switch checked={!!v} onChange={(val) => patchReviewItem(r.id, { include: val })} />
                },
                {
                  title: "Title",
                  dataIndex: "edited_title",
                  render: (_v, r) => (
                    <Input value={r.edited_title || r.title} onChange={(e) => patchReviewItem(r.id, { edited_title: e.target.value })} />
                  )
                }
              ]}
            />
          </Space>
        </Card>
      )}

      {coiLoad && (
        <Card title="COI Load Result">
          <pre>{JSON.stringify(coiLoad, null, 2)}</pre>
        </Card>
      )}
    </Space>
  );
}
