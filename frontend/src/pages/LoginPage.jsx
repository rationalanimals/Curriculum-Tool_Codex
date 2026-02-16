import { useState } from "react";
import { Button, Card, Input, Space, Typography } from "antd";

const API = "http://127.0.0.1:8000";

export function LoginPage() {
  const [username, setUsername] = useState("design_admin");
  const [password, setPassword] = useState("design_admin");
  const [token, setToken] = useState(localStorage.getItem("session_token") || "");
  const [error, setError] = useState("");

  async function submit() {
    setError("");
    const r = await fetch(`${API}/auth/login`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password })
    });
    if (!r.ok) {
      setError("Login failed.");
      return;
    }
    const data = await r.json();
    localStorage.setItem("session_token", data.session_token);
    setToken(data.session_token);
  }

  return (
    <Card title="CMT Login" style={{ maxWidth: 480 }}>
      <Space direction="vertical" style={{ width: "100%" }}>
        <Input value={username} onChange={(e) => setUsername(e.target.value)} placeholder="Username" />
        <Input.Password value={password} onChange={(e) => setPassword(e.target.value)} placeholder="Password" />
        <Button type="primary" onClick={submit}>
          Login
        </Button>
        {error && <Typography.Text type="danger">{error}</Typography.Text>}
        {token && <Typography.Text copyable>{token}</Typography.Text>}
      </Space>
    </Card>
  );
}
