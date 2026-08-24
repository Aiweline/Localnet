import { StrictMode, useCallback, useEffect, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import { getCurrentWindow } from "@tauri-apps/api/window";
import { open, save } from "@tauri-apps/plugin-dialog";
import { isPermissionGranted, requestPermission, sendNotification } from "@tauri-apps/plugin-notification";
import { openPath, openUrl } from "@tauri-apps/plugin-opener";
import {
  AlertCircle, BellRing, Building2, Check, CheckCheck, ChevronRight, CircleUserRound,
  File, FileDown, FolderOpen, Image as ImageIcon, LoaderCircle, Mail, MessageCircleMore,
  Monitor, Paperclip, RefreshCw, Search, Send, Settings2, ShieldCheck, UserPlus,
  UsersRound, Wifi, WifiOff, X,
} from "lucide-react";
import { mergePresenceSnapshot, startSnapshotReconciliation } from "./presence";
import "./styles.css";

type Platform = "windows" | "macos" | "unknown";
type Direction = "incoming" | "outgoing";
type FriendRequestStatus = "pending" | "accepted" | "rejected";
type MessageKind = "text" | "image" | "file";
type MessageStatus = "sending" | "delivered" | "failed";
type TransferKind = "image" | "file";
type TransferStatus = "awaitingAcceptance" | "transferring" | "completed" | "cancelled" | "failed";

interface LocalProfile { peerId: string; nickname: string; platform: Platform; protocolVersion: number }
interface PeerSummary { peerId: string; nickname: string; platform: Platform; online: boolean; protocolVersion: number; lastSeen: string }
interface FriendRequest { requestId: string; peerId: string; nickname: string; direction: Direction; status: FriendRequestStatus; createdAt: string; updatedAt: string }
interface Friend { peerId: string; nickname: string; platform: Platform; online: boolean; addedAt: string; lastSeen: string }
interface ChatMessage { messageId: string; peerId: string; direction: Direction; kind: MessageKind; body?: string; localPath?: string; fileName?: string; fileSize?: number; status: MessageStatus; error?: string; createdAt: string }
interface TransferRecord { transferId: string; peerId: string; direction: Direction; kind: TransferKind; fileName: string; fileSize: number; mimeType: string; sha256: string; localPath?: string; transferredBytes: number; status: TransferStatus; error?: string; createdAt: string; updatedAt: string }
interface TransferPreferences { autoReceiveFiles: boolean; receiveDirectory: string }
interface PresenceSnapshot { peers: PeerSummary[]; friends: Friend[] }
interface BootstrapSnapshot { localProfile: LocalProfile | null; transferPreferences: TransferPreferences; peers: PeerSummary[]; friendRequests: FriendRequest[]; friends: Friend[]; messages: ChatMessage[]; transfers: TransferRecord[] }
interface ToastState { tone: "success" | "error" | "info"; message: string }
type NetworkEvent =
  | { type: "peerDiscovered"; peer: PeerSummary }
  | { type: "peerOffline"; peerId: string; lastSeen: string }
  | { type: "friendRequestReceived"; request: FriendRequest }
  | { type: "friendRequestDelivered"; requestId: string }
  | { type: "friendRequestResolved"; request: FriendRequest; friend: Friend | null }
  | { type: "messageReceived"; message: ChatMessage }
  | { type: "messageStatusChanged"; messageId: string; status: MessageStatus; error: string | null }
  | { type: "transferUpdated"; transfer: TransferRecord }
  | { type: "networkError"; code: string; message: string };

const EMPTY_SNAPSHOT: BootstrapSnapshot = { localProfile: null, transferPreferences: { autoReceiveFiles: false, receiveDirectory: "" }, peers: [], friendRequests: [], friends: [], messages: [], transfers: [] };

function App() {
  const [snapshot, setSnapshot] = useState<BootstrapSnapshot>(EMPTY_SNAPSHOT);
  const [loading, setLoading] = useState(true);
  const [fatalError, setFatalError] = useState("");
  const [selectedPeerId, setSelectedPeerId] = useState<string | null>(null);
  const [searchText, setSearchText] = useState("");
  const [messageText, setMessageText] = useState("");
  const [busyKey, setBusyKey] = useState("");
  const [toast, setToast] = useState<ToastState | null>(null);
  const [editingProfile, setEditingProfile] = useState(false);
  const [discoverySlow, setDiscoverySlow] = useState(false);
  const [announcement, setAnnouncement] = useState("");
  const [notificationPermission, setNotificationPermission] = useState<NotificationPermission>("default");
  const refreshTimer = useRef<number | null>(null);
  const hasOnlinePeer = snapshot.peers.some((peer) => peer.online);

  const refresh = useCallback(async (showLoader = false) => {
    if (showLoader) setLoading(true);
    try {
      const next = await invoke<BootstrapSnapshot>("bootstrap");
      setSnapshot(next);
      setFatalError("");
    } catch (error) {
      setFatalError(errorMessage(error));
    } finally {
      if (showLoader) setLoading(false);
    }
  }, []);

  const scheduleRefresh = useCallback(() => {
    if (refreshTimer.current !== null) window.clearTimeout(refreshTimer.current);
    refreshTimer.current = window.setTimeout(() => void refresh(), 80);
  }, [refresh]);

  const reconcilePresence = useCallback(async () => {
    const presence = await invoke<PresenceSnapshot>("presence");
    setSnapshot((current) => mergePresenceSnapshot(current, presence));
  }, []);

  const handleNetworkEvent = useCallback((event: NetworkEvent) => {
    scheduleRefresh();
    switch (event.type) {
      case "friendRequestReceived":
        setAnnouncement(`${event.request.nickname} 发来了好友申请`);
        void notifyIncomingFriendRequest(event.request);
        break;
      case "friendRequestDelivered":
        setToast({ tone: "success", message: "好友申请已送达，正在等待对方处理" });
        break;
      case "friendRequestResolved":
        if (event.friend) setSelectedPeerId(event.friend.peerId);
        break;
      case "transferUpdated":
        if (event.transfer.direction === "incoming" && event.transfer.status === "completed") {
          setAnnouncement(`文件 ${event.transfer.fileName} 已接收完成`);
          void notifyIncomingTransfer(event.transfer);
        }
        break;
      case "networkError":
        setToast({ tone: "error", message: event.message });
        break;
      default:
        break;
    }
  }, [scheduleRefresh]);

  useEffect(() => {
    void refresh(true);
    let disposed = false;
    let unlisten: (() => void) | undefined;
    void listen<NetworkEvent>("localnet://event", ({ payload }) => handleNetworkEvent(payload)).then((cleanup) => {
      if (disposed) cleanup(); else unlisten = cleanup;
    });
    return () => {
      disposed = true;
      unlisten?.();
      if (refreshTimer.current !== null) window.clearTimeout(refreshTimer.current);
    };
  }, [handleNetworkEvent, refresh]);

  useEffect(() => startSnapshotReconciliation(reconcilePresence), [reconcilePresence]);

  useEffect(() => {
    let active = true;
    void isPermissionGranted()
      .then((granted) => active && setNotificationPermission(granted ? "granted" : "default"))
      .catch(() => active && setNotificationPermission("default"));
    return () => { active = false; };
  }, []);

  useEffect(() => {
    if (!toast) return;
    const timer = window.setTimeout(() => setToast(null), 3600);
    return () => window.clearTimeout(timer);
  }, [toast]);

  useEffect(() => {
    if (selectedPeerId && snapshot.friends.some((friend) => friend.peerId === selectedPeerId)) return;
    setSelectedPeerId(snapshot.friends[0]?.peerId ?? null);
  }, [selectedPeerId, snapshot.friends]);

  useEffect(() => {
    if (!snapshot.localProfile || hasOnlinePeer) {
      setDiscoverySlow(false);
      return;
    }
    const timer = window.setTimeout(() => setDiscoverySlow(true), 12_000);
    return () => window.clearTimeout(timer);
  }, [hasOnlinePeer, snapshot.localProfile]);

  const act = useCallback(async <T,>(key: string, action: () => Promise<T>, success?: string): Promise<T | null> => {
    setBusyKey(key);
    try {
      const result = await action();
      if (success) setToast({ tone: "success", message: success });
      await refresh();
      return result;
    } catch (error) {
      setToast({ tone: "error", message: errorMessage(error) });
      return null;
    } finally {
      setBusyKey("");
    }
  }, [refresh]);

  const enableSystemNotifications = async () => {
    setBusyKey("notifications");
    try {
      const granted = await isPermissionGranted();
      const permission = granted ? "granted" : await requestPermission();
      setNotificationPermission(permission);
      setToast(permission === "granted"
        ? { tone: "success", message: "系统通知已开启" }
        : { tone: "info", message: "系统通知未开启；应用内好友申请提醒仍然有效" });
    } catch (error) {
      setToast({ tone: "error", message: errorMessage(error) });
    } finally {
      setBusyKey("");
    }
  };

  if (loading) return <BootScreen />;
  if (fatalError && !snapshot.localProfile) return <FatalScreen message={fatalError} onRetry={() => void refresh(true)} />;
  if (!snapshot.localProfile) {
    return <Onboarding busy={busyKey === "onboarding"} onSubmit={async (nickname) => {
      await act("onboarding", () => invoke("complete_onboarding", { nickname }), "Weline Localnet 已准备好，正在发现附近用户");
    }} />;
  }

  const profile = snapshot.localProfile;
  const selectedFriend = snapshot.friends.find((friend) => friend.peerId === selectedPeerId) ?? null;
  const friendIds = new Set(snapshot.friends.map((friend) => friend.peerId));
  const pendingOutgoingPeerIds = new Set(snapshot.friendRequests.filter((request) => request.direction === "outgoing" && request.status === "pending").map((request) => request.peerId));
  const nearbyPeers = snapshot.peers.filter((peer) => peer.online && !friendIds.has(peer.peerId) && peer.peerId !== profile.peerId);
  const incomingRequests = snapshot.friendRequests.filter((request) => request.direction === "incoming" && request.status === "pending");
  const primaryIncomingRequest = incomingRequests[0] ?? null;
  const normalizedSearch = searchText.trim().toLocaleLowerCase();
  const visibleFriends = snapshot.friends.filter((friend) => friend.nickname.toLocaleLowerCase().includes(normalizedSearch));
  const visibleNearby = nearbyPeers.filter((peer) => peer.nickname.toLocaleLowerCase().includes(normalizedSearch));

  const resolveFriend = async (requestId: string, accepted: boolean) => {
    const result = await act(`request:${requestId}`, () => invoke("resolve_friend_request", { requestId, accepted }), accepted ? "已添加好友，可以开始聊天了" : "已拒绝好友申请");
    if (accepted && result) {
      const request = incomingRequests.find((item) => item.requestId === requestId);
      if (request) setSelectedPeerId(request.peerId);
    }
  };

  const sendText = async () => {
    if (!selectedFriend || !messageText.trim()) return;
    const body = messageText;
    setMessageText("");
    const result = await act("send-text", () => invoke("send_text", { peerId: selectedFriend.peerId, body }));
    if (!result) setMessageText(body);
  };

  const chooseAndSend = async (kind: TransferKind) => {
    if (!selectedFriend) return;
    const selected = await open({
      multiple: false,
      directory: false,
      filters: kind === "image" ? [{ name: "图片", extensions: ["png", "jpg", "jpeg", "gif", "webp", "bmp", "heic", "heif"] }] : undefined,
    });
    if (typeof selected !== "string") return;
    await act(`send-${kind}`, () => invoke("send_file", { peerId: selectedFriend.peerId, path: selected, kind }), kind === "image" ? "图片已加入发送队列" : "文件请求已发送");
  };

  const resolveTransfer = async (transfer: TransferRecord, accepted: boolean) => {
    let savePath: string | null = null;
    if (accepted) {
      savePath = await save({ defaultPath: transfer.fileName });
      if (!savePath) return;
    }
    await act(`transfer:${transfer.transferId}`, () => invoke("resolve_transfer", { transferId: transfer.transferId, accepted, savePath }), accepted ? "已接受文件，正在内网传输" : "已拒绝文件");
  };

  return (
    <main className="app-shell">
      <aside className="sidebar">
        <header className="brand-row">
          <span className="brand-mark"><MessageCircleMore size={22} /></span>
          <span><strong>Weline Localnet</strong><small>局域网私密传输</small></span>
          <button className="icon-button" title="设置" onClick={() => setEditingProfile(true)}><Settings2 size={18} /></button>
        </header>
        <button className="profile-card" onClick={() => setEditingProfile(true)}>
          <Avatar name={profile.nickname} online />
          <span><strong>{profile.nickname}</strong><small><ShieldCheck size={13} /> 本机身份已保护</small></span>
          <ChevronRight size={16} />
        </button>
        <label className="search-box">
          <Search size={16} />
          <input value={searchText} onChange={(event) => setSearchText(event.target.value)} placeholder="搜索用户" aria-label="搜索用户" />
          {searchText && <button onClick={() => setSearchText("")} aria-label="清除搜索"><X size={14} /></button>}
        </label>
        <div className="sidebar-scroll">
          {incomingRequests.length > 0 && (
            <SidebarSection title="新的好友申请" count={incomingRequests.length} accent>
              {incomingRequests.map((request) => (
                <div className="request-card" key={request.requestId}>
                  <Avatar name={request.nickname} online />
                  <span><strong>{request.nickname}</strong><small>想添加你为好友</small></span>
                  <div className="request-actions">
                    <button className="mini-button reject" disabled={busyKey === `request:${request.requestId}`} onClick={() => void resolveFriend(request.requestId, false)} title="拒绝"><X size={14} /></button>
                    <button className="mini-button accept" disabled={busyKey === `request:${request.requestId}`} onClick={() => void resolveFriend(request.requestId, true)} title="接受">
                      {busyKey === `request:${request.requestId}` ? <LoaderCircle size={14} className="spin" /> : <Check size={14} />}
                    </button>
                  </div>
                </div>
              ))}
            </SidebarSection>
          )}
          <SidebarSection title="附近用户" count={visibleNearby.length} icon={<Wifi size={14} />}>
            {visibleNearby.map((peer) => (
              <div className="person-row" key={peer.peerId}>
                <Avatar name={peer.nickname} online />
                <span><strong>{peer.nickname}</strong><small>{platformLabel(peer.platform)} · 在线</small></span>
                <button className="add-button" disabled={busyKey === `add:${peer.peerId}` || peer.protocolVersion !== 1 || pendingOutgoingPeerIds.has(peer.peerId)} title={pendingOutgoingPeerIds.has(peer.peerId) ? "好友申请正在等待对方处理" : peer.protocolVersion === 1 ? "添加好友" : "版本不兼容"} onClick={() => void act(`add:${peer.peerId}`, () => invoke("send_friend_request", { peerId: peer.peerId }))}>
                  {busyKey === `add:${peer.peerId}` ? <LoaderCircle size={15} className="spin" /> : pendingOutgoingPeerIds.has(peer.peerId) ? <Check size={15} /> : <UserPlus size={15} />}
                </button>
              </div>
            ))}
            {visibleNearby.length === 0 && <SidebarEmpty icon={discoverySlow ? <AlertCircle size={17} /> : <Wifi size={17} />} text={searchText ? "没有匹配的附近用户" : discoverySlow ? "仍未发现？请允许 Weline Localnet 访问本地网络，并检查 Windows 防火墙" : "正在自动发现同一内网的用户"} />}
          </SidebarSection>
          <SidebarSection title="好友" count={visibleFriends.length} icon={<UsersRound size={14} />}>
            {visibleFriends.map((friend) => (
              <button key={friend.peerId} className={`friend-row ${friend.peerId === selectedPeerId ? "active" : ""}`} onClick={() => setSelectedPeerId(friend.peerId)}>
                <Avatar name={friend.nickname} online={friend.online} />
                <span><strong>{friend.nickname}</strong><small>{friend.online ? "在线" : `离线 · ${relativeTime(friend.lastSeen)}`}</small></span>
                {friend.online ? <span className="online-dot" /> : <WifiOff size={14} />}
              </button>
            ))}
            {visibleFriends.length === 0 && <SidebarEmpty icon={<CircleUserRound size={17} />} text={searchText ? "没有匹配的好友" : "添加附近用户后会显示在这里"} />}
          </SidebarSection>
        </div>
      </aside>

      <section className="content-shell">
        {primaryIncomingRequest && (
          <IncomingRequestBanner
            request={primaryIncomingRequest}
            platform={snapshot.peers.find((peer) => peer.peerId === primaryIncomingRequest.peerId)?.platform ?? "unknown"}
            count={incomingRequests.length}
            busy={busyKey === `request:${primaryIncomingRequest.requestId}`}
            onResolve={(accepted) => void resolveFriend(primaryIncomingRequest.requestId, accepted)}
          />
        )}
        {selectedFriend ? (
          <Conversation
            friend={selectedFriend}
            messages={snapshot.messages.filter((message) => message.peerId === selectedFriend.peerId)}
            transfers={snapshot.transfers.filter((transfer) => transfer.peerId === selectedFriend.peerId)}
            messageText={messageText}
            setMessageText={setMessageText}
            busyKey={busyKey}
            onSendText={sendText}
            onSendImage={() => void chooseAndSend("image")}
            onSendFile={() => void chooseAndSend("file")}
            onRetry={(messageId) => void act(`retry:${messageId}`, () => invoke("retry_text", { messageId }))}
            onResolveTransfer={resolveTransfer}
            onCancelTransfer={(transferId) => void act(`transfer:${transferId}`, () => invoke("cancel_transfer", { transferId }), "已取消文件传输")}
          />
        ) : <WelcomePanel nearbyCount={nearbyPeers.length} friendCount={snapshot.friends.length} />}
      </section>

      {editingProfile && (
        <ProfileDialog profile={profile} transferPreferences={snapshot.transferPreferences} busy={busyKey === "profile"} directoryBusy={busyKey === "open-receive-directory"} notificationBusy={busyKey === "notifications"} notificationPermission={notificationPermission} onEnableNotifications={enableSystemNotifications} onChooseDirectory={async (currentDirectory) => {
          const selected = await open({ multiple: false, directory: true, defaultPath: currentDirectory || undefined });
          return typeof selected === "string" ? selected : null;
        }} onOpenDirectory={async (path) => {
          await act("open-receive-directory", () => openPath(path));
        }} onClose={() => setEditingProfile(false)} onSave={async (nickname, transferPreferences) => {
          const result = await act("profile", async () => {
            return invoke("update_settings", {
              nickname,
              autoReceiveFiles: transferPreferences.autoReceiveFiles,
              receiveDirectory: transferPreferences.receiveDirectory,
            });
          }, "设置已保存");
          if (result) setEditingProfile(false);
        }} />
      )}
      <span className="sr-only" aria-live="polite">{announcement}</span>
      {toast && <Toast toast={toast} onClose={() => setToast(null)} />}
    </main>
  );
}

function IncomingRequestBanner({ request, platform, count, busy, onResolve }: {
  request: FriendRequest;
  platform: Platform;
  count: number;
  busy: boolean;
  onResolve: (accepted: boolean) => void;
}) {
  return (
    <section className="incoming-request-banner" role="region" aria-label="新的好友申请">
      <span className="request-banner-icon"><BellRing size={21} /></span>
      <span className="request-banner-copy">
        <small>新的好友申请 · {platformLabel(platform)}</small>
        <strong>{request.nickname} 想添加你为好友</strong>
        <span>确认后即可互发文字、图片和文件。</span>
      </span>
      {count > 1 && <span className="request-banner-count">另有 {count - 1} 条</span>}
      <div className="request-banner-actions">
        <button className="request-banner-reject" disabled={busy} onClick={() => onResolve(false)}>拒绝</button>
        <button className="request-banner-accept" disabled={busy} onClick={() => onResolve(true)}>
          {busy ? <LoaderCircle size={15} className="spin" /> : <Check size={15} />} 接受
        </button>
      </div>
    </section>
  );
}

function Conversation({ friend, messages, transfers, messageText, setMessageText, busyKey, onSendText, onSendImage, onSendFile, onRetry, onResolveTransfer, onCancelTransfer }: {
  friend: Friend; messages: ChatMessage[]; transfers: TransferRecord[]; messageText: string; setMessageText: (value: string) => void; busyKey: string; onSendText: () => void; onSendImage: () => void; onSendFile: () => void; onRetry: (messageId: string) => void; onResolveTransfer: (transfer: TransferRecord, accepted: boolean) => void; onCancelTransfer: (transferId: string) => void;
}) {
  const bottomRef = useRef<HTMLDivElement>(null);
  const transferMap = new Map(transfers.map((transfer) => [transfer.transferId, transfer]));
  const messageIds = new Set(messages.map((message) => message.messageId));
  const timeline = [
    ...messages.map((message) => ({ type: "message" as const, createdAt: message.createdAt, message })),
    ...transfers.filter((transfer) => !messageIds.has(transfer.transferId)).map((transfer) => ({ type: "transfer" as const, createdAt: transfer.createdAt, transfer })),
  ].sort((left, right) => left.createdAt.localeCompare(right.createdAt));
  const updates = transfers.map((transfer) => transfer.updatedAt).join("|");

  useEffect(() => { bottomRef.current?.scrollIntoView({ block: "end" }); }, [timeline.length, updates]);
  const pending = !friend.online;
  return (
    <section className="conversation">
      <header className="conversation-header">
        <Avatar name={friend.nickname} online={friend.online} large />
        <span><strong>{friend.nickname}</strong><small className={friend.online ? "is-online" : ""}>{friend.online ? <><Wifi size={13} /> 在线 · 内网直连</> : <><WifiOff size={13} /> 当前离线</>}</small></span>
        <div className="secure-pill"><ShieldCheck size={15} /> 加密连接</div>
      </header>
      <div className="message-scroll">
        <div className="privacy-note"><ShieldCheck size={14} /> 消息和文件只在本地网络中传输</div>
        {timeline.length === 0 && <div className="conversation-empty"><span><MessageCircleMore size={31} /></span><strong>向 {friend.nickname} 打个招呼</strong><p>可以发送文字、图片和文件，不经过互联网服务器。</p></div>}
        {timeline.map((item) => item.type === "message" ? (
          <MessageBubble key={item.message.messageId} message={item.message} transfer={transferMap.get(item.message.messageId)} busy={busyKey === `retry:${item.message.messageId}` || busyKey === `transfer:${item.message.messageId}`} onRetry={onRetry} onCancelTransfer={onCancelTransfer} />
        ) : (
          <TransferOfferCard key={item.transfer.transferId} transfer={item.transfer} busy={busyKey === `transfer:${item.transfer.transferId}`} onResolve={onResolveTransfer} />
        ))}
        <div ref={bottomRef} />
      </div>
      <footer className="composer-wrap">
        {pending && <div className="offline-banner"><WifiOff size={14} /> 对方离线，启动 Weline Localnet 后即可继续发送</div>}
        <div className="composer-tools">
          <button onClick={onSendImage} disabled={pending || busyKey === "send-image"}>{busyKey === "send-image" ? <LoaderCircle size={18} className="spin" /> : <ImageIcon size={18} />}图片</button>
          <button onClick={onSendFile} disabled={pending || busyKey === "send-file"}>{busyKey === "send-file" ? <LoaderCircle size={18} className="spin" /> : <Paperclip size={18} />}文件</button>
          <span>Enter 发送 · Shift + Enter 换行</span>
        </div>
        <div className="composer">
          <textarea value={messageText} onChange={(event) => setMessageText(event.target.value)} onKeyDown={(event) => {
            if (event.key === "Enter" && !event.shiftKey) { event.preventDefault(); if (!pending && busyKey !== "send-text") onSendText(); }
          }} placeholder={pending ? "对方当前离线" : `发消息给 ${friend.nickname}`} disabled={pending} rows={1} />
          <button className="send-button" onClick={onSendText} disabled={pending || busyKey === "send-text" || !messageText.trim()} aria-label="发送消息">{busyKey === "send-text" ? <LoaderCircle size={19} className="spin" /> : <Send size={19} />}</button>
        </div>
      </footer>
    </section>
  );
}

function MessageBubble({ message, transfer, busy, onRetry, onCancelTransfer }: { message: ChatMessage; transfer?: TransferRecord; busy: boolean; onRetry: (messageId: string) => void; onCancelTransfer: (transferId: string) => void }) {
  const outgoing = message.direction === "outgoing";
  return (
    <div className={`message-line ${outgoing ? "outgoing" : "incoming"}`}>
      <div className={`message-bubble ${message.kind !== "text" ? "attachment-bubble" : ""}`}>
        {message.kind === "text" ? <p>{message.body}</p> : message.kind === "image" ? <ImageMessage message={message} /> : <FileMessage message={message} />}
        {transfer && transfer.status !== "completed" && <TransferProgress transfer={transfer} />}
        <div className="message-meta"><span>{formatTime(message.createdAt)}</span>{outgoing && <MessageStatusIcon status={message.status} />}</div>
        {message.status === "failed" && <div className="message-error"><AlertCircle size={13} /><span>{message.error || transfer?.error || "发送失败"}</span>{message.kind === "text" && <button disabled={busy} onClick={() => onRetry(message.messageId)}>{busy ? <LoaderCircle size={12} className="spin" /> : <RefreshCw size={12} />} 重试</button>}</div>}
        {transfer?.direction === "outgoing" && transfer.status === "awaitingAcceptance" && <button className="cancel-link" disabled={busy} onClick={() => onCancelTransfer(transfer.transferId)}>取消发送</button>}
      </div>
    </div>
  );
}

function ImageMessage({ message }: { message: ChatMessage }) {
  const [preview, setPreview] = useState<string | null>(null);
  useEffect(() => {
    let active = true;
    if (message.status === "delivered") void invoke<string | null>("image_preview", { messageId: message.messageId }).then((value) => active && setPreview(value)).catch(() => undefined);
    return () => { active = false; };
  }, [message.messageId, message.status]);
  return <div className="image-message">{preview ? <img src={preview} alt={message.fileName || "收到的图片"} /> : <div className="image-placeholder"><ImageIcon size={30} /><span>{message.fileName}</span></div>}{message.status === "delivered" && message.localPath && <button onClick={() => void openPath(message.localPath!)}>打开原图</button>}</div>;
}

function FileMessage({ message }: { message: ChatMessage }) {
  return <div className="file-message"><span className="file-icon"><File size={22} /></span><span><strong>{message.fileName || "文件"}</strong><small>{formatBytes(message.fileSize || 0)}</small></span>{message.status === "delivered" && message.localPath && <button title="打开文件" onClick={() => void openPath(message.localPath!)}><FileDown size={17} /></button>}</div>;
}

function TransferOfferCard({ transfer, busy, onResolve }: { transfer: TransferRecord; busy: boolean; onResolve: (transfer: TransferRecord, accepted: boolean) => void }) {
  const incoming = transfer.direction === "incoming";
  return <div className={`message-line ${incoming ? "incoming" : "outgoing"}`}><div className="transfer-card"><span className="file-icon"><FileDown size={22} /></span><span><small>{incoming ? "收到文件" : "正在发送"}</small><strong>{transfer.fileName}</strong><em>{formatBytes(transfer.fileSize)}</em></span>{incoming && transfer.status === "awaitingAcceptance" ? <div className="transfer-actions"><button disabled={busy} onClick={() => onResolve(transfer, false)}>拒绝</button><button className="primary" disabled={busy} onClick={() => onResolve(transfer, true)}>{busy ? <LoaderCircle size={14} className="spin" /> : <FileDown size={14} />} 接收</button></div> : <TransferProgress transfer={transfer} />}</div></div>;
}

function TransferProgress({ transfer }: { transfer: TransferRecord }) {
  const percent = transfer.fileSize === 0 ? 100 : Math.min(100, Math.round((transfer.transferredBytes / transfer.fileSize) * 100));
  const label: Record<TransferStatus, string> = { awaitingAcceptance: transfer.direction === "outgoing" ? "等待对方接收" : "等待确认", transferring: `传输中 ${percent}%`, completed: "已完成", cancelled: "已取消", failed: "传输失败" };
  return <div className={`transfer-progress ${transfer.status}`}><div><span style={{ width: `${percent}%` }} /></div><small>{label[transfer.status]}</small></div>;
}

function MessageStatusIcon({ status }: { status: MessageStatus }) {
  if (status === "sending") return <LoaderCircle size={12} className="spin" />;
  if (status === "failed") return <AlertCircle size={12} className="status-error" />;
  return <CheckCheck size={13} />;
}

function SidebarSection({ title, count, icon, accent, children }: { title: string; count: number; icon?: React.ReactNode; accent?: boolean; children: React.ReactNode }) {
  return <section className={`sidebar-section ${accent ? "accent" : ""}`}><header>{icon}<span>{title}</span><em>{count}</em></header>{children}</section>;
}
function SidebarEmpty({ icon, text }: { icon: React.ReactNode; text: string }) { return <div className="sidebar-empty">{icon}<span>{text}</span></div>; }
function Avatar({ name, online, large }: { name: string; online: boolean; large?: boolean }) {
  const hue = [...name].reduce((sum, character) => sum + character.charCodeAt(0), 0) % 360;
  return <span className={`avatar ${large ? "large" : ""}`} style={{ "--avatar-hue": hue } as React.CSSProperties}>{name.trim().charAt(0).toLocaleUpperCase() || "L"}<i className={online ? "online" : "offline"} /></span>;
}

function WelcomePanel({ nearbyCount, friendCount }: { nearbyCount: number; friendCount: number }) {
  return <section className="welcome-panel"><div className="welcome-art"><span className="pulse pulse-one" /><span className="pulse pulse-two" /><span className="welcome-logo"><MessageCircleMore size={40} /></span></div><p className="eyebrow">LOCAL · PRIVATE · FAST</p><h1>和身边的人直接连接</h1><p>Weline Localnet 会自动发现同一内网中的用户。添加好友后，即可发送文字、图片和文件。</p><div className="welcome-stats"><span><Wifi size={18} /><strong>{nearbyCount}</strong><small>附近在线</small></span><span><UsersRound size={18} /><strong>{friendCount}</strong><small>我的好友</small></span><span><ShieldCheck size={18} /><strong>本地</strong><small>数据传输</small></span></div></section>;
}

function Onboarding({ busy, onSubmit }: { busy: boolean; onSubmit: (nickname: string) => Promise<void> }) {
  const [nickname, setNickname] = useState("");
  return <main className="onboarding-screen"><section className="onboarding-card"><div className="onboarding-copy"><span className="brand-mark large"><MessageCircleMore size={30} /></span><p className="eyebrow">欢迎使用 WELINE LOCALNET</p><h1>无需服务器，直接和内网用户连接。</h1><p>设置一个昵称，其他 Weline Localnet 用户就能在附近列表中发现你。</p><ul><li><Wifi size={17} /> 自动发现同一局域网用户</li><li><ShieldCheck size={17} /> 设备身份保存在本机</li><li><Paperclip size={17} /> 文字、图片和文件直传</li></ul></div><form className="nickname-form" onSubmit={(event) => { event.preventDefault(); if (nickname.trim()) void onSubmit(nickname); }}><span className="form-icon"><CircleUserRound size={25} /></span><h2>你希望别人怎么称呼你？</h2><p>昵称只会显示给同一内网中的 Weline Localnet 用户。</p><label>昵称<input autoFocus maxLength={32} value={nickname} onChange={(event) => setNickname(event.target.value)} placeholder="例如：小林" /></label><button className="primary-button" disabled={busy || !nickname.trim()}>{busy ? <LoaderCircle size={18} className="spin" /> : <Wifi size={18} />}{busy ? "正在启动…" : "进入 Weline Localnet"}</button><small><ShieldCheck size={13} /> 不需要账号、手机号或互联网连接</small></form></section></main>;
}

function ProfileDialog({ profile, transferPreferences, busy, directoryBusy, notificationBusy, notificationPermission, onEnableNotifications, onChooseDirectory, onOpenDirectory, onClose, onSave }: {
  profile: LocalProfile;
  transferPreferences: TransferPreferences;
  busy: boolean;
  directoryBusy: boolean;
  notificationBusy: boolean;
  notificationPermission: NotificationPermission;
  onEnableNotifications: () => Promise<void>;
  onChooseDirectory: (currentDirectory: string) => Promise<string | null>;
  onOpenDirectory: (path: string) => Promise<void>;
  onClose: () => void;
  onSave: (nickname: string, transferPreferences: TransferPreferences) => Promise<void>;
}) {
  const [nickname, setNickname] = useState(profile.nickname);
  const [autoReceiveFiles, setAutoReceiveFiles] = useState(transferPreferences.autoReceiveFiles);
  const [receiveDirectory, setReceiveDirectory] = useState(transferPreferences.receiveDirectory);
  const notificationEnabled = notificationPermission === "granted";
  const chooseDirectory = async () => {
    const selected = await onChooseDirectory(receiveDirectory);
    if (selected) setReceiveDirectory(selected);
  };
  return (
    <div className="dialog-backdrop" role="presentation" onMouseDown={(event) => event.target === event.currentTarget && onClose()}>
      <form className="dialog-card settings-dialog" role="dialog" aria-modal="true" aria-labelledby="profile-title" onSubmit={(event) => { event.preventDefault(); if (nickname.trim() && (!autoReceiveFiles || receiveDirectory.trim())) void onSave(nickname, { autoReceiveFiles, receiveDirectory }); }}>
        <button type="button" className="dialog-close" onClick={onClose}><X size={18} /></button>
        <Avatar name={nickname || profile.nickname} online large />
        <h2 id="profile-title">本机设置</h2>
        <p>管理附近用户看到的昵称，以及文件接收方式。</p>
        <label>昵称<input autoFocus maxLength={32} value={nickname} onChange={(event) => setNickname(event.target.value)} /></label>
        <div className="device-id"><Monitor size={15} /><span><small>设备身份</small><code>{shortPeerId(profile.peerId)}</code></span></div>
        <div className="auto-receive-setting">
          <FileDown size={17} />
          <span><strong>自动接收好友文件</strong><small>仅自动接收已添加好友发送的图片和文件。</small></span>
          <button type="button" className={autoReceiveFiles ? "enabled" : ""} role="switch" aria-checked={autoReceiveFiles} aria-label="自动接收好友文件" onClick={() => setAutoReceiveFiles((enabled) => !enabled)}><i /></button>
        </div>
        <div className="receive-directory-setting">
          <FolderOpen size={17} />
          <span><strong>文件接收位置</strong><code title={receiveDirectory}>{receiveDirectory}</code></span>
          <div>
            <button type="button" disabled={!receiveDirectory || directoryBusy} onClick={() => void onOpenDirectory(receiveDirectory)} title="打开接收文件夹">{directoryBusy ? "打开中…" : "打开"}</button>
            <button type="button" onClick={() => void chooseDirectory()}>更改</button>
          </div>
        </div>
        <div className="notification-setting">
          <BellRing size={17} />
          <span><strong>系统通知</strong><small>显示好友申请和文件接收完成提醒；拒绝授权不影响应用内提示。</small></span>
          <button type="button" disabled={notificationBusy || notificationEnabled} onClick={() => void onEnableNotifications()}>
            {notificationBusy ? <LoaderCircle size={13} className="spin" /> : notificationEnabled ? <Check size={13} /> : null}
            {notificationEnabled ? "已开启" : notificationPermission === "denied" ? "重新授权" : "开启"}
          </button>
        </div>
        <div className="about-setting">
          <Building2 size={17} />
          <span><strong>成都阿玛云科技有限公司</strong><button type="button" onClick={() => void openUrl("mailto:contact@amayum.com")}><Mail size={12} /> contact@amayum.com</button></span>
        </div>
        <button className="primary-button" disabled={busy || !nickname.trim() || (autoReceiveFiles && !receiveDirectory.trim())}>{busy && <LoaderCircle size={16} className="spin" />} 保存设置</button>
      </form>
    </div>
  );
}

function BootScreen() { return <main className="boot-screen"><section className="boot-card"><div className="boot-mark"><MessageCircleMore size={34} /></div><p className="eyebrow">LOCAL · PRIVATE · FAST</p><h1>Weline Localnet</h1><p>正在准备你的局域网通信空间…</p><div className="loading-track"><span /></div></section></main>; }
function FatalScreen({ message, onRetry }: { message: string; onRetry: () => void }) { return <main className="boot-screen"><section className="boot-card fatal-card"><div className="boot-mark error"><AlertCircle size={32} /></div><h1>Weline Localnet 无法启动</h1><p>{message}</p><button className="primary-button" onClick={onRetry}><RefreshCw size={17} /> 重试</button></section></main>; }
function Toast({ toast, onClose }: { toast: ToastState; onClose: () => void }) { return <div className={`toast ${toast.tone}`} role="status">{toast.tone === "success" ? <Check size={17} /> : toast.tone === "error" ? <AlertCircle size={17} /> : <Wifi size={17} />}<span>{toast.message}</span><button onClick={onClose}><X size={14} /></button></div>; }

function errorMessage(error: unknown): string {
  if (typeof error === "string") return error;
  if (error && typeof error === "object") {
    if ("message" in error && typeof error.message === "string") return error.message;
    try { return JSON.stringify(error); } catch { return "操作失败，请稍后重试"; }
  }
  return "操作失败，请稍后重试";
}
async function notifyIncomingFriendRequest(request: FriendRequest): Promise<void> {
  try {
    if (await getCurrentWindow().isFocused() || !(await isPermissionGranted())) return;
    sendNotification({
      title: "Weline Localnet",
      body: `${request.nickname} 想添加你为好友。打开 Weline Localnet 接受或拒绝。`,
    });
  } catch (error) {
    console.debug("Native friend-request notification unavailable", error);
  }
}
async function notifyIncomingTransfer(transfer: TransferRecord): Promise<void> {
  try {
    if (await getCurrentWindow().isFocused() || !(await isPermissionGranted())) return;
    sendNotification({
      title: "Weline Localnet",
      body: `${transfer.fileName} 已接收完成。`,
    });
  } catch (error) {
    console.debug("Native file-received notification unavailable", error);
  }
}
function platformLabel(platform: Platform): string { return platform === "windows" ? "Windows" : platform === "macos" ? "macOS" : "桌面设备"; }
function shortPeerId(peerId: string): string { return peerId.length > 20 ? `${peerId.slice(0, 9)}…${peerId.slice(-7)}` : peerId; }
function formatTime(value: string): string { const date = new Date(value); return Number.isNaN(date.getTime()) ? "" : date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }); }
function relativeTime(value: string): string { const milliseconds = Date.now() - new Date(value).getTime(); if (!Number.isFinite(milliseconds) || milliseconds < 60_000) return "刚刚"; const minutes = Math.floor(milliseconds / 60_000); if (minutes < 60) return `${minutes} 分钟前`; const hours = Math.floor(minutes / 60); return hours < 24 ? `${hours} 小时前` : `${Math.floor(hours / 24)} 天前`; }
function formatBytes(bytes: number): string { if (bytes < 1024) return `${bytes} B`; if (bytes < 1024 ** 2) return `${(bytes / 1024).toFixed(1)} KiB`; if (bytes < 1024 ** 3) return `${(bytes / 1024 ** 2).toFixed(1)} MiB`; return `${(bytes / 1024 ** 3).toFixed(2)} GiB`; }

createRoot(document.getElementById("root")!).render(<StrictMode><App /></StrictMode>);
