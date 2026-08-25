import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import { attachmentActionState } from "../src/file-actions.ts";

test("delivered image and file messages expose open, save-as, and reveal actions", () => {
  for (const kind of ["image", "file"] as const) {
    assert.deepEqual(
      attachmentActionState({
        kind,
        status: "delivered",
        localPath: "C:\\Users\\Me\\Downloads\\quarterly report.pdf",
        fileName: "quarterly report.pdf",
      }),
      {
        available: true,
        defaultFileName: "quarterly report.pdf",
      },
    );
  }
});

test("text, unfinished, failed, and pathless messages expose no local file actions", () => {
  for (const input of [
    { kind: "text", status: "delivered", localPath: "C:\\private.txt", fileName: "private.txt" },
    { kind: "file", status: "sending", localPath: "C:\\private.txt", fileName: "private.txt" },
    { kind: "image", status: "failed", localPath: "C:\\private.png", fileName: "private.png" },
    { kind: "file", status: "delivered", localPath: "", fileName: "private.txt" },
  ] as const) {
    assert.deepEqual(attachmentActionState(input), {
      available: false,
      defaultFileName: "attachment",
    });
  }
});

test("save-as uses a safe filename fallback without translating user filenames", () => {
  assert.deepEqual(
    attachmentActionState({
      kind: "file",
      status: "delivered",
      localPath: "/Users/me/Downloads/report.bin",
      fileName: "  ",
    }),
    { available: true, defaultFileName: "attachment" },
  );
});

test("the main window can open and reveal an attachment through the scoped opener plugin", async () => {
  const capability = JSON.parse(
    await readFile(new URL("../src-tauri/capabilities/default.json", import.meta.url), "utf8"),
  );
  assert.ok(capability.permissions.includes("opener:allow-open-path"));
  assert.ok(capability.permissions.includes("opener:default"));
});
