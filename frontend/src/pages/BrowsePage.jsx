import { useMemo, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Alert, Button, Card, Col, List, Row, Space, Typography } from "antd";

const API = "http://127.0.0.1:8000";

function useAuthedFetch(path) {
  const token = localStorage.getItem("session_token");
  return useQuery({
    queryKey: [path, token],
    queryFn: async () => {
      const r = await fetch(`${API}${path}${path.includes("?") ? "&" : "?"}session_token=${encodeURIComponent(token || "")}`);
      if (!r.ok) throw new Error("Request failed");
      return r.json();
    }
  });
}

export function BrowsePage() {
  const qc = useQueryClient();
  const [seedSummary, setSeedSummary] = useState(null);
  const [seedError, setSeedError] = useState("");
  const [seeding, setSeeding] = useState(false);
  const versionsQ = useAuthedFetch("/versions");
  const activeVersionId = useMemo(
    () => versionsQ.data?.find((v) => v.status === "ACTIVE")?.id,
    [versionsQ.data]
  );
  const coursesQ = useAuthedFetch(`/courses${activeVersionId ? `?version_id=${activeVersionId}` : ""}`);
  const programsQ = useAuthedFetch("/programs");
  const requirementsQ = useAuthedFetch("/requirements");
  const instructorsQ = useAuthedFetch("/instructors");
  const classroomsQ = useAuthedFetch("/classrooms");
  const sectionsQ = useAuthedFetch(`/sections${activeVersionId ? `?version_id=${activeVersionId}` : ""}`);
  const cadetsQ = useAuthedFetch("/cadets");

  async function loadDemoData() {
    setSeedError("");
    setSeedSummary(null);
    setSeeding(true);
    try {
      const token = localStorage.getItem("session_token") || "";
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 15000);
      const r = await fetch(`${API}/demo/load-data?session_token=${encodeURIComponent(token)}`, {
        method: "POST",
        signal: controller.signal
      });
      clearTimeout(timeoutId);
      if (!r.ok) throw new Error(await r.text());
      const data = await r.json();
      setSeedSummary(data.summary || {});
      // Trigger refreshes in background so the button doesn't appear hung on slow retries.
      versionsQ.refetch();
      coursesQ.refetch();
      programsQ.refetch();
      requirementsQ.refetch();
      instructorsQ.refetch();
      classroomsQ.refetch();
      sectionsQ.refetch();
      cadetsQ.refetch();
      qc.invalidateQueries({ queryKey: ["/versions"] });
      qc.invalidateQueries({ queryKey: ["/courses"] });
      qc.invalidateQueries({ queryKey: ["/programs"] });
      qc.invalidateQueries({ queryKey: ["/requirements"] });
      qc.invalidateQueries({ queryKey: ["/instructors"] });
      qc.invalidateQueries({ queryKey: ["/classrooms"] });
      qc.invalidateQueries({ queryKey: ["/sections"] });
      qc.invalidateQueries({ queryKey: ["/cadets"] });
      qc.invalidateQueries({ queryKey: ["versions"] });
      qc.invalidateQueries({ queryKey: ["courses"] });
      qc.invalidateQueries({ queryKey: ["programs"] });
      qc.invalidateQueries({ queryKey: ["requirements"] });
      qc.invalidateQueries({ queryKey: ["canvas"] });
      qc.invalidateQueries({ queryKey: ["validation"] });
    } catch (e) {
      if (e?.name === "AbortError") {
        setSeedError("Demo seed request timed out after 15s. Check backend terminal for DB lock or restart backend.");
      } else {
        setSeedError(String(e.message || e));
      }
    } finally {
      setSeeding(false);
    }
  }

  return (
    <Row gutter={16}>
      <Col span={24} style={{ marginBottom: 12 }}>
        <Card title="QC Data Loader">
          <Space direction="vertical">
            <Button type="primary" onClick={loadDemoData} loading={seeding}>
              Load Demo Data
            </Button>
            <Typography.Text type="secondary">
              Seeds an idempotent Phase 2 demo dataset if records are missing, then refreshes Browse and Design views.
            </Typography.Text>
            {seedSummary && <Alert type="success" message="Demo data loaded" description={<pre>{JSON.stringify(seedSummary, null, 2)}</pre>} />}
            {seedError && <Alert type="error" message="Demo seed failed" description={seedError} />}
          </Space>
        </Card>
      </Col>
      <Col span={12}>
        <Card title="Curriculum Versions">
          <List dataSource={versionsQ.data || []} renderItem={(v) => <List.Item>{v.name} ({v.status})</List.Item>} />
        </Card>
      </Col>
      <Col span={12}>
        <Card title="Courses">
          <List dataSource={coursesQ.data || []} renderItem={(c) => <List.Item>{c.course_number}: {c.title}</List.Item>} />
        </Card>
      </Col>
      <Col span={12} style={{ marginTop: 16 }}>
        <Card title="Programs">
          <List dataSource={programsQ.data || []} renderItem={(p) => <List.Item>{p.name}</List.Item>} />
        </Card>
      </Col>
      <Col span={12} style={{ marginTop: 16 }}>
        <Card title="Requirements">
          <List dataSource={requirementsQ.data || []} renderItem={(r) => <List.Item>{r.name}</List.Item>} />
        </Card>
      </Col>
      <Col span={24} style={{ marginTop: 12 }}>
        <Typography.Text type="secondary">
          Phase 1 shell: login, navigation, and baseline data browsing views including courses, instructors, classrooms, sections, and cadets.
        </Typography.Text>
      </Col>
      <Col span={12} style={{ marginTop: 16 }}>
        <Card title="Instructors">
          <List dataSource={instructorsQ.data || []} renderItem={(i) => <List.Item>{i.name} ({i.department || "N/A"})</List.Item>} />
        </Card>
      </Col>
      <Col span={12} style={{ marginTop: 16 }}>
        <Card title="Classrooms">
          <List dataSource={classroomsQ.data || []} renderItem={(r) => <List.Item>{r.building} {r.room_number} (cap {r.capacity})</List.Item>} />
        </Card>
      </Col>
      <Col span={12} style={{ marginTop: 16 }}>
        <Card title="Sections">
          <List dataSource={sectionsQ.data || []} renderItem={(s) => <List.Item>{s.semester_label} - {s.course_id}</List.Item>} />
        </Card>
      </Col>
      <Col span={12} style={{ marginTop: 16 }}>
        <Card title="Cadets">
          <List dataSource={cadetsQ.data || []} renderItem={(c) => <List.Item>{c.name} (C/O {c.class_year})</List.Item>} />
        </Card>
      </Col>
    </Row>
  );
}
