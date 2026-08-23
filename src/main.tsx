import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { MessageCircleMore } from "lucide-react";
import "./styles.css";

function App() {
  return (
    <main className="boot-screen">
      <section className="boot-card" aria-labelledby="localnet-title">
        <div className="boot-mark" aria-hidden="true">
          <MessageCircleMore size={34} strokeWidth={2.1} />
        </div>
        <p className="eyebrow">LOCAL · PRIVATE · FAST</p>
        <h1 id="localnet-title">Localnet</h1>
        <p>正在准备你的局域网通信空间…</p>
        <div className="loading-track" aria-label="正在启动">
          <span />
        </div>
      </section>
    </main>
  );
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
