import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter, Link, Navigate, Route, Routes, useLocation } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { Layout, Menu } from "antd";
import "antd/dist/reset.css";
import "./styles.css";

import { LoginPage } from "./pages/LoginPage";
import { BrowsePage } from "./pages/BrowsePage";
import { DesignStudioPage } from "./pages/DesignStudioPage";
import { ImportPage } from "./pages/ImportPage";

const queryClient = new QueryClient();
const { Header, Content } = Layout;

function AppShell() {
  const location = useLocation();
  const selectedKey = location.pathname.startsWith("/browse")
    ? "browse"
    : location.pathname.startsWith("/design")
      ? "design"
      : location.pathname.startsWith("/import")
        ? "import"
        : "login";
  return (
    <Layout style={{ minHeight: "100vh" }}>
      <Header>
        <Menu
          theme="dark"
          mode="horizontal"
          selectedKeys={[selectedKey]}
          items={[
            { key: "browse", label: <Link to="/browse">Browse</Link> },
            { key: "design", label: <Link to="/design">Design Studio</Link> },
            { key: "import", label: <Link to="/import">Import</Link> },
            { key: "login", label: <Link to="/login">Login</Link> }
          ]}
        />
      </Header>
      <Content style={{ padding: 24 }}>
        <Routes>
          <Route path="/login" element={<LoginPage />} />
          <Route path="/browse" element={<BrowsePage />} />
          <Route path="/design" element={<DesignStudioPage />} />
          <Route path="/import" element={<ImportPage />} />
          <Route path="*" element={<Navigate to="/login" replace />} />
        </Routes>
      </Content>
    </Layout>
  );
}

ReactDOM.createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <AppShell />
      </BrowserRouter>
    </QueryClientProvider>
  </React.StrictMode>
);
