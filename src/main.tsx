import { StrictMode, useCallback, useEffect, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import { getVersion } from "@tauri-apps/api/app";
import { getCurrentWindow } from "@tauri-apps/api/window";
import { open, save } from "@tauri-apps/plugin-dialog";
import { isPermissionGranted, requestPermission, sendNotification } from "@tauri-apps/plugin-notification";
import { openPath, openUrl, revealItemInDir } from "@tauri-apps/plugin-opener";
import {
  AlertCircle, BellRing, Building2, Check, CheckCheck, ChevronRight, CircleUserRound,
  ExternalLink, File, FileDown, FolderOpen, Image as ImageIcon, LoaderCircle, Mail, MessageCircleMore,
  Monitor, Paperclip, RefreshCw, Search, Send, Settings2, ShieldCheck, UserPlus,
  UsersRound, Wifi, WifiOff, X,
} from "lucide-react";
import { submitDiscoveryRefresh } from "./discovery-refresh";
import { attachmentActionState } from "./file-actions";
import {
  reconcileLanguageSnapshot, resolveLanguagePreference, translate,
  type LanguagePreference, type TranslationKey,
} from "./i18n/core";
import { I18nProvider, useI18n } from "./i18n/react";
import {
  initializeNotificationPermission,
  requestNotificationPermission,
  type NotificationPermissionState,
} from "./notifications";
import {
  mergePeerDiscoverySnapshot, mergePeerOfflineSnapshot, mergePresenceSnapshot,
  mergeResolvedFriendSnapshot, nearbyPeerEntries, startSnapshotReconciliation,
} from "./presence";
import { transferStatusPresentation, type TransferStatusInput, type TransferStatusLabels } from "./transfer-status";
import { checkForUpdate, createUpdateDownloadRequest, type UpdateInfo } from "./update";
import "./styles.css";

type Platform = "windows" | "macos" | "unknown";
type Direction = "incoming" | "outgoing";
type FriendRequestStatus = "pending" | "accepted" | "rejected";
type MessageKind = "text" | "image" | "file";
type MessageStatus = "sending" | "delivered" | "failed";
type TransferKind = "image" | "file";

interface LocalProfile { peerId: string; nickname: string; platform: Platform; protocolVersion: number }
interface PeerSummary { peerId: string; nickname: string; platform: Platform; online: boolean; protocolVersion: number; lastSeen: string }
interface FriendRequest { requestId: string; peerId: string; nickname: string; direction: Direction; status: FriendRequestStatus; createdAt: string; updatedAt: string }
interface Friend { peerId: string; nickname: string; platform: Platform; online: boolean; addedAt: string; lastSeen: string }
interface ChatMessage { messageId: string; peerId: string; direction: Direction; kind: MessageKind; body?: string; localPath?: string; fileName?: string; fileSize?: number; status: MessageStatus; error?: string; createdAt: string }
interface TransferRecord extends TransferStatusInput { transferId: string; peerId: string; kind: TransferKind; fileName: string; mimeType: string; sha256: string; localPath?: string; createdAt: string; updatedAt: string }
interface TransferPreferences { autoReceiveFiles: boolean; receiveDirectory: string }
interface PresenceSnapshot { peers: PeerSummary[]; friends: Friend[] }
interface BootstrapSnapshot { localProfile: LocalProfile | null; languagePreference: string; transferPreferences: TransferPreferences; peers: PeerSummary[]; friendRequests: FriendRequest[]; friends: Friend[]; messages: ChatMessage[]; transfers: TransferRecord[] }
interface ToastState { tone: "success" | "error" | "info"; message: string }
interface DownloadedUpdate { path: string; bytes: number }
interface UpdateDownloadProgress { version: string; downloadedBytes: number; totalBytes: number }
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

const EMPTY_SNAPSHOT: BootstrapSnapshot = { localProfile: null, languagePreference: "auto", transferPreferences: { autoReceiveFiles: false, receiveDirectory: "" }, peers: [], friendRequests: [], friends: [], messages: [], transfers: [] };

function App() {
  const { t, preference, setLanguagePreference, relativeTime } = useI18n();
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
  const [discoveryRefreshing, setDiscoveryRefreshing] = useState(false);
  const [announcement, setAnnouncement] = useState("");
  const [appVersion, setAppVersion] = useState("");
  const [updateInfo, setUpdateInfo] = useState<UpdateInfo | null>(null);
  const [updateChecked, setUpdateChecked] = useState(false);
  const [updateChecking, setUpdateChecking] = useState(false);
  const [updateDownloading, setUpdateDownloading] = useState(false);
  const [updateDownloaded, setUpdateDownloaded] = useState(false);
  const [updateProgress, setUpdateProgress] = useState(0);
  const [updateError, setUpdateError] = useState("");
  const [notificationPermission, setNotificationPermission] = useState<NotificationPermissionState>("default");
  const refreshTimer = useRef<number | null>(null);
  const languagePreferenceRef = useRef<LanguagePreference | null>(null);
  const translatorRef = useRef(t);
  const hasOnlinePeer = snapshot.peers.some((peer) => peer.online);

  useEffect(() => {
    translatorRef.current = t;
  }, [t]);

  const refresh = useCallback(async (showLoader = false) => {
    if (showLoader) setLoading(true);
    try {
      const next = await invoke<BootstrapSnapshot>("bootstrap");
      const effectivePreference = reconcileLanguageSnapshot(
        next.languagePreference,
        languagePreferenceRef.current,
      );
      languagePreferenceRef.current = effectivePreference;
      setLanguagePreference(effectivePreference);
      setSnapshot({ ...next, languagePreference: effectivePreference });
      setFatalError("");
    } catch (error) {
      setFatalError(localizedError(error, translatorRef.current));
    } finally {
      if (showLoader) setLoading(false);
    }
  }, [setLanguagePreference]);

  const scheduleRefresh = useCallback(() => {
    if (refreshTimer.current !== null) window.clearTimeout(refreshTimer.current);
    refreshTimer.current = window.setTimeout(() => void refresh(), 80);
  }, [refresh]);

  const reconcilePresence = useCallback(async () => {
    const presence = await invoke<PresenceSnapshot>("presence");
    setSnapshot((current) => mergePresenceSnapshot(current, presence));
  }, []);

  const handleNetworkEvent = useCallback((event: NetworkEvent) => {
    const currentT = translatorRef.current;
    if (event.type === "peerDiscovered") {
      setSnapshot((current) => mergePeerDiscoverySnapshot(current, event.peer));
      return;
    }
    if (event.type === "peerOffline") {
      setSnapshot((current) => mergePeerOfflineSnapshot(current, event.peerId, event.lastSeen));
      return;
    }
    scheduleRefresh();
    switch (event.type) {
      case "friendRequestReceived":
        setAnnouncement(currentT("friends.requestFrom", { name: event.request.nickname }));
        void notifyIncomingFriendRequest(event.request, currentT);
        break;
      case "friendRequestDelivered":
        setToast({ tone: "success", message: currentT("friends.requestDelivered") });
        break;
      case "friendRequestResolved":
        if (event.friend) {
          const friend = event.friend;
          setSnapshot((current) => mergeResolvedFriendSnapshot(current, event.request, friend));
          setSelectedPeerId(friend.peerId);
        }
        break;
      case "transferUpdated":
        if (event.transfer.direction === "incoming" && event.transfer.status === "completed") {
          setAnnouncement(currentT("transfer.receivedComplete", { name: event.transfer.fileName }));
          void notifyIncomingTransfer(event.transfer, currentT);
        }
        break;
      case "networkError":
        setToast({ tone: "error", message: localizedError(event, currentT) });
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
    void getVersion()
      .then((version) => { if (active) setAppVersion(version); })
      .catch(() => { if (active) setAppVersion(""); });
    return () => { active = false; };
  }, []);

  const profilePlatform = snapshot.localProfile?.platform;
  const runUpdateCheck = useCallback(async (showFeedback = false) => {
    if (!appVersion || !profilePlatform) return;
    setUpdateChecking(true);
    setUpdateError("");
    try {
      const update = await checkForUpdate(appVersion, profilePlatform);
      setUpdateInfo(update);
      setUpdateChecked(true);
      setUpdateDownloaded(false);
      setUpdateProgress(0);
      if (showFeedback) {
        setToast(update
          ? { tone: "info", message: t("update.found", { version: update.version }) }
          : { tone: "success", message: t("update.latest") });
      }
    } catch {
      setUpdateChecked(true);
      setUpdateError(t("update.checkFailedShort"));
      if (showFeedback) setToast({ tone: "error", message: t("update.checkFailed") });
    } finally {
      setUpdateChecking(false);
    }
  }, [appVersion, profilePlatform, t]);

  useEffect(() => {
    if (appVersion && profilePlatform) void runUpdateCheck();
  }, [appVersion, profilePlatform, runUpdateCheck]);

  useEffect(() => {
    let disposed = false;
    let unlisten: (() => void) | undefined;
    void listen<UpdateDownloadProgress>("localnet://update-progress", ({ payload }) => {
      if (!disposed && updateInfo?.version === payload.version && payload.totalBytes > 0) {
        setUpdateProgress(Math.min(100, Math.round((payload.downloadedBytes / payload.totalBytes) * 100)));
      }
    }).then((cleanup) => {
      if (disposed) cleanup(); else unlisten = cleanup;
    });
    return () => {
      disposed = true;
      unlisten?.();
    };
  }, [updateInfo?.version]);

  useEffect(() => {
    let active = true;
    void initializeNotificationPermission({
      isPermissionGranted,
      requestPermission,
      store: window.localStorage,
    }).then((permission) => {
      if (active) setNotificationPermission(permission);
    });
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
      setToast({ tone: "error", message: localizedError(error, t) });
      return null;
    } finally {
      setBusyKey("");
    }
  }, [refresh, t]);

  const enableSystemNotifications = async () => {
    setBusyKey("notifications");
    try {
      const permission = await requestNotificationPermission({
        isPermissionGranted,
        requestPermission,
        store: window.localStorage,
      });
      setNotificationPermission(permission);
      setToast(permission === "granted"
        ? { tone: "success", message: t("notifications.enabled") }
        : { tone: "info", message: t("notifications.notEnabled") });
    } catch (error) {
      setToast({ tone: "error", message: localizedError(error, t) });
    } finally {
      setBusyKey("");
    }
  };

  const changeLanguagePreference = async (nextPreference: LanguagePreference, showSuccess = false) => {
    languagePreferenceRef.current = nextPreference;
    setLanguagePreference(nextPreference);
    setSnapshot((current) => ({ ...current, languagePreference: nextPreference }));
    setBusyKey("language");
    try {
      await invoke("update_language_preference", { languagePreference: nextPreference });
      if (showSuccess) {
        const nextLocale = resolveLanguagePreference(nextPreference, navigator.languages);
        setToast({ tone: "success", message: translate(nextLocale, "settings.languageSaved") });
      }
    } catch (error) {
      const nextLocale = resolveLanguagePreference(nextPreference, navigator.languages);
      const nextTranslator: Translator = (key, params) => translate(nextLocale, key, params);
      setToast({ tone: "error", message: localizedError(error, nextTranslator) });
    } finally {
      setBusyKey("");
    }
  };

  const refreshNearby = async () => {
    try {
      await submitDiscoveryRefresh({
        triggerNetworkDiscovery: () => invoke("refresh_discovery"),
        refreshSnapshot: reconcilePresence,
        setRefreshing: setDiscoveryRefreshing,
      });
      setDiscoverySlow(false);
      setToast({ tone: "info", message: t("nearby.refreshStarted") });
    } catch (error) {
      setToast({ tone: "error", message: localizedError(error, t) });
    }
  };

  const downloadAvailableUpdate = async () => {
    if (!updateInfo || updateDownloading) return;
    setUpdateDownloading(true);
    setUpdateDownloaded(false);
    setUpdateError("");
    setUpdateProgress(0);
    try {
      const request = createUpdateDownloadRequest(updateInfo);
      await invoke<DownloadedUpdate>("download_update", { request });
      setUpdateProgress(100);
      setUpdateDownloaded(true);
      setToast({ tone: "success", message: t("update.downloaded") });
    } catch (error) {
      const message = localizedError(error, t);
      setUpdateError(message);
      setToast({ tone: "error", message });
    } finally {
      setUpdateDownloading(false);
    }
  };

  const openAvailableUpdate = async () => {
    if (!updateInfo || !updateDownloaded) return;
    try {
      const request = createUpdateDownloadRequest(updateInfo);
      await invoke("open_downloaded_update", { request });
      setToast({ tone: "info", message: t("update.opened") });
    } catch (error) {
      setToast({ tone: "error", message: localizedError(error, t) });
    }
  };

  if (loading) return <BootScreen />;
  if (fatalError && !snapshot.localProfile) return <FatalScreen message={fatalError} onRetry={() => void refresh(true)} />;
  if (!snapshot.localProfile) {
    return <Onboarding busy={busyKey === "onboarding"} languageBusy={busyKey === "language"} onLanguageChange={(nextPreference) => changeLanguagePreference(nextPreference)} onSubmit={async (nickname) => {
      await act("onboarding", () => invoke("complete_onboarding", { nickname }));
    }} />;
  }

  const profile = snapshot.localProfile;
  const selectedFriend = snapshot.friends.find((friend) => friend.peerId === selectedPeerId) ?? null;
  const nearbyPeers = nearbyPeerEntries(snapshot.peers, snapshot.friends, snapshot.friendRequests, profile.peerId);
  const incomingRequests = snapshot.friendRequests.filter((request) => request.direction === "incoming" && request.status === "pending");
  const primaryIncomingRequest = incomingRequests[0] ?? null;
  const normalizedSearch = searchText.trim().toLocaleLowerCase();
  const visibleFriends = snapshot.friends.filter((friend) => friend.nickname.toLocaleLowerCase().includes(normalizedSearch));
  const visibleNearby = nearbyPeers.filter(({ peer }) => peer.nickname.toLocaleLowerCase().includes(normalizedSearch));

  const resolveFriend = async (requestId: string, accepted: boolean) => {
    const result = await act(`request:${requestId}`, () => invoke("resolve_friend_request", { requestId, accepted }), accepted ? t("friends.added") : t("friends.rejected"));
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
      filters: kind === "image" ? [{ name: t("picker.images"), extensions: ["png", "jpg", "jpeg", "gif", "webp", "bmp", "heic", "heif"] }] : undefined,
    });
    if (typeof selected !== "string") return;
    await act(`send-${kind}`, () => invoke("send_file", { peerId: selectedFriend.peerId, path: selected, kind }), kind === "image" ? t("chat.sentImage") : t("chat.sentFile"));
  };

  const openAttachment = async (message: ChatMessage) => {
    if (!attachmentActionState(message).available || !message.localPath) return;
    setBusyKey(`attachment:${message.messageId}:open`);
    try {
      await openPath(message.localPath);
    } catch {
      setToast({ tone: "error", message: t("transfer.openFailed") });
    } finally {
      setBusyKey("");
    }
  };

  const revealAttachment = async (message: ChatMessage) => {
    if (!attachmentActionState(message).available || !message.localPath) return;
    setBusyKey(`attachment:${message.messageId}:reveal`);
    try {
      await revealItemInDir(message.localPath);
    } catch {
      setToast({ tone: "error", message: t("transfer.revealFailed") });
    } finally {
      setBusyKey("");
    }
  };

  const saveAttachmentAs = async (message: ChatMessage) => {
    const actionState = attachmentActionState(message);
    if (!actionState.available) return;
    const destinationPath = await save({ defaultPath: actionState.defaultFileName });
    if (!destinationPath) return;
    setBusyKey(`attachment:${message.messageId}:save`);
    try {
      await invoke<number>("save_message_file_as", {
        messageId: message.messageId,
        destinationPath,
      });
      const savedName = destinationPath.split(/[\\/]/).pop() || actionState.defaultFileName;
      setToast({ tone: "success", message: t("transfer.saveSuccess", { name: savedName }) });
    } catch {
      setToast({ tone: "error", message: t("transfer.saveFailed") });
    } finally {
      setBusyKey("");
    }
  };

  const resolveTransfer = async (transfer: TransferRecord, accepted: boolean) => {
    let savePath: string | null = null;
    if (accepted) {
      savePath = await save({ defaultPath: transfer.fileName });
      if (!savePath) return;
    }
    await act(`transfer:${transfer.transferId}`, () => invoke("resolve_transfer", { transferId: transfer.transferId, accepted, savePath }), accepted ? t("transfer.accepted") : t("transfer.rejected"));
  };

  return (
    <main className="app-shell">
      <aside className="sidebar">
        <header className="brand-row">
          <span className="brand-mark"><MessageCircleMore size={22} /></span>
          <span><strong>Weline Localnet {appVersion && <span className="version-badge">v{appVersion}</span>}</strong><small>{t("app.subtitle")}</small></span>
          <button className="icon-button" title={t("common.settings")} aria-label={t("common.settings")} onClick={() => setEditingProfile(true)}><Settings2 size={18} /></button>
        </header>
        <button className="profile-card" onClick={() => setEditingProfile(true)}>
          <Avatar name={profile.nickname} online />
          <span><strong>{profile.nickname}</strong><small><ShieldCheck size={13} /> {t("app.identityProtected")}</small></span>
          <ChevronRight size={16} />
        </button>
        <label className="search-box">
          <Search size={16} />
          <input value={searchText} onChange={(event) => setSearchText(event.target.value)} placeholder={t("common.searchUsers")} aria-label={t("common.searchUsers")} />
          {searchText && <button onClick={() => setSearchText("")} aria-label={t("common.clearSearch")}><X size={14} /></button>}
        </label>
        <div className="sidebar-scroll">
          {incomingRequests.length > 0 && (
            <SidebarSection title={t("friends.newRequests")} count={incomingRequests.length} accent>
              {incomingRequests.map((request) => (
                <div className="request-card" key={request.requestId}>
                  <Avatar name={request.nickname} online />
                  <span><strong>{request.nickname}</strong><small>{t("friends.wantsToAdd")}</small></span>
                  <div className="request-actions">
                    <button className="mini-button reject" disabled={busyKey === `request:${request.requestId}`} onClick={() => void resolveFriend(request.requestId, false)} title={t("common.reject")}><X size={14} /></button>
                    <button className="mini-button accept" disabled={busyKey === `request:${request.requestId}`} onClick={() => void resolveFriend(request.requestId, true)} title={t("common.accept")}>
                      {busyKey === `request:${request.requestId}` ? <LoaderCircle size={14} className="spin" /> : <Check size={14} />}
                    </button>
                  </div>
                </div>
              ))}
            </SidebarSection>
          )}
          <SidebarSection
            title={t("nearby.title")}
            count={visibleNearby.length}
            icon={<Wifi size={14} />}
            headerAction={(
              <button
                type="button"
                className="section-action"
                disabled={discoveryRefreshing}
                onClick={() => void refreshNearby()}
                title={t("nearby.rescan")}
                aria-label={t("nearby.rescan")}
              >
                <RefreshCw size={13} className={discoveryRefreshing ? "spin" : ""} />
              </button>
            )}
          >
            {visibleNearby.map(({ peer, relationship }) => (
              <div className="person-row" key={peer.peerId}>
                <Avatar name={peer.nickname} online />
                <span><strong>{peer.nickname}</strong><small>{platformLabel(peer.platform, t)} · {t("common.online")}</small></span>
                <button className="add-button" disabled={busyKey === `add:${peer.peerId}` || relationship === "pending"} title={relationship === "pending" ? t("friends.pending") : t("friends.add")} onClick={() => void act(`add:${peer.peerId}`, () => invoke("send_friend_request", { peerId: peer.peerId }))}>
                  {busyKey === `add:${peer.peerId}` ? <LoaderCircle size={15} className="spin" /> : relationship === "pending" ? <Check size={15} /> : <UserPlus size={15} />}
                </button>
              </div>
            ))}
            {visibleNearby.length === 0 && <SidebarEmpty icon={discoverySlow ? <AlertCircle size={17} /> : <Wifi size={17} />} text={searchText ? t("nearby.noMatch") : discoverySlow ? t("nearby.slow") : t("nearby.scanning")} />}
          </SidebarSection>
          <SidebarSection title={t("friends.title")} count={visibleFriends.length} icon={<UsersRound size={14} />}>
            {visibleFriends.map((friend) => (
              <button key={friend.peerId} className={`friend-row ${friend.peerId === selectedPeerId ? "active" : ""}`} onClick={() => setSelectedPeerId(friend.peerId)}>
                <Avatar name={friend.nickname} online={friend.online} />
                <span><strong>{friend.nickname}</strong><small>{friend.online ? t("common.online") : t("friends.offlineSince", { time: relativeTime(friend.lastSeen) })}</small></span>
                {friend.online ? <span className="online-dot" /> : <WifiOff size={14} />}
              </button>
            ))}
            {visibleFriends.length === 0 && <SidebarEmpty icon={<CircleUserRound size={17} />} text={searchText ? t("friends.noMatch") : t("friends.empty")} />}
          </SidebarSection>
        </div>
      </aside>

      <section className="content-shell">
        {updateInfo && (
          <UpdateBanner
            update={updateInfo}
            downloading={updateDownloading}
            downloaded={updateDownloaded}
            progress={updateProgress}
            error={updateError}
            onDownload={() => void downloadAvailableUpdate()}
            onOpen={() => void openAvailableUpdate()}
          />
        )}
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
            onCancelTransfer={(transferId) => void act(`transfer:${transferId}`, () => invoke("cancel_transfer", { transferId }), t("transfer.cancelSuccess"))}
            onOpenAttachment={(message) => void openAttachment(message)}
            onSaveAttachment={(message) => void saveAttachmentAs(message)}
            onRevealAttachment={(message) => void revealAttachment(message)}
          />
        ) : <WelcomePanel nearbyCount={nearbyPeers.length} friendCount={snapshot.friends.length} />}
      </section>

      {editingProfile && (
        <ProfileDialog profile={profile} languagePreference={preference} languageBusy={busyKey === "language"} onLanguageChange={(nextPreference) => changeLanguagePreference(nextPreference, true)} transferPreferences={snapshot.transferPreferences} busy={busyKey === "profile"} directoryBusy={busyKey === "open-receive-directory"} notificationBusy={busyKey === "notifications"} notificationPermission={notificationPermission} appVersion={appVersion} updateChecking={updateChecking} updateStatus={updateInfo ? t("update.found", { version: updateInfo.version }) : updateError || (updateChecked ? t("update.latest") : t("update.notChecked"))} onCheckUpdates={() => runUpdateCheck(true)} onEnableNotifications={enableSystemNotifications} onChooseDirectory={async (currentDirectory) => {
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
          }, t("settings.saved"));
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
  const { t } = useI18n();
  return (
    <section className="incoming-request-banner" role="region" aria-label={t("friends.newRequests")}>
      <span className="request-banner-icon"><BellRing size={21} /></span>
      <span className="request-banner-copy">
        <small>{t("friends.newRequests")} · {platformLabel(platform, t)}</small>
        <strong>{t("friends.requestFrom", { name: request.nickname })}</strong>
        <span>{t("friends.requestInstruction")}</span>
      </span>
      {count > 1 && <span className="request-banner-count">{t("friends.moreRequests", { count: count - 1 })}</span>}
      <div className="request-banner-actions">
        <button className="request-banner-reject" disabled={busy} onClick={() => onResolve(false)}>{t("common.reject")}</button>
        <button className="request-banner-accept" disabled={busy} onClick={() => onResolve(true)}>
          {busy ? <LoaderCircle size={15} className="spin" /> : <Check size={15} />} {t("common.accept")}
        </button>
      </div>
    </section>
  );
}

function UpdateBanner({ update, downloading, downloaded, progress, error, onDownload, onOpen }: {
  update: UpdateInfo;
  downloading: boolean;
  downloaded: boolean;
  progress: number;
  error: string;
  onDownload: () => void;
  onOpen: () => void;
}) {
  const { t } = useI18n();
  return (
    <section className="update-banner" role="region" aria-label={t("update.applicationUpdate")}>
      <span className="update-banner-icon"><FileDown size={19} /></span>
      <span className="update-banner-copy">
        <strong>{t("update.found", { version: update.version })}</strong>
        <small>{error || (downloaded ? t("update.verified") : downloading ? t("update.downloading", { progress }) : t("update.official"))}</small>
        {downloading && <span role="progressbar" aria-label={t("update.downloading", { progress })} aria-valuemin={0} aria-valuemax={100} aria-valuenow={progress}><i style={{ width: `${progress}%` }} /></span>}
      </span>
      <button type="button" disabled={downloading} onClick={downloaded ? onOpen : onDownload}>
        {downloading ? <LoaderCircle size={14} className="spin" /> : downloaded ? <FolderOpen size={14} /> : <FileDown size={14} />}
        {downloading ? t("update.downloading", { progress }) : downloaded ? t("update.openInstaller") : error ? t("update.redownload") : t("update.download")}
      </button>
    </section>
  );
}

function Conversation({ friend, messages, transfers, messageText, setMessageText, busyKey, onSendText, onSendImage, onSendFile, onRetry, onResolveTransfer, onCancelTransfer, onOpenAttachment, onSaveAttachment, onRevealAttachment }: {
  friend: Friend; messages: ChatMessage[]; transfers: TransferRecord[]; messageText: string; setMessageText: (value: string) => void; busyKey: string; onSendText: () => void; onSendImage: () => void; onSendFile: () => void; onRetry: (messageId: string) => void; onResolveTransfer: (transfer: TransferRecord, accepted: boolean) => void; onCancelTransfer: (transferId: string) => void; onOpenAttachment: (message: ChatMessage) => void; onSaveAttachment: (message: ChatMessage) => void; onRevealAttachment: (message: ChatMessage) => void;
}) {
  const { t } = useI18n();
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
        <span><strong>{friend.nickname}</strong><small className={friend.online ? "is-online" : ""}>{friend.online ? <><Wifi size={13} /> {t("chat.directOnline")}</> : <><WifiOff size={13} /> {t("chat.currentlyOffline")}</>}</small></span>
        <div className="secure-pill"><ShieldCheck size={15} /> {t("chat.encrypted")}</div>
      </header>
      <div className="message-scroll">
        <div className="privacy-note"><ShieldCheck size={14} /> {t("chat.privacy")}</div>
        {timeline.length === 0 && <div className="conversation-empty"><span><MessageCircleMore size={31} /></span><strong>{t("chat.sayHello", { name: friend.nickname })}</strong><p>{t("chat.description")}</p></div>}
        {timeline.map((item) => item.type === "message" ? (
          <MessageBubble key={item.message.messageId} message={item.message} transfer={transferMap.get(item.message.messageId)} busy={busyKey === `retry:${item.message.messageId}` || busyKey === `transfer:${item.message.messageId}`} attachmentBusy={busyKey.startsWith(`attachment:${item.message.messageId}:`)} onRetry={onRetry} onCancelTransfer={onCancelTransfer} onOpenAttachment={onOpenAttachment} onSaveAttachment={onSaveAttachment} onRevealAttachment={onRevealAttachment} />
        ) : (
          <TransferOfferCard key={item.transfer.transferId} transfer={item.transfer} busy={busyKey === `transfer:${item.transfer.transferId}`} onResolve={onResolveTransfer} onCancel={onCancelTransfer} />
        ))}
        <div ref={bottomRef} />
      </div>
      <footer className="composer-wrap">
        {pending && <div className="offline-banner"><WifiOff size={14} /> {t("chat.offlineHint")}</div>}
        <div className="composer-tools">
          <button onClick={onSendImage} disabled={pending || busyKey === "send-image"}>{busyKey === "send-image" ? <LoaderCircle size={18} className="spin" /> : <ImageIcon size={18} />}{t("common.image")}</button>
          <button onClick={onSendFile} disabled={pending || busyKey === "send-file"}>{busyKey === "send-file" ? <LoaderCircle size={18} className="spin" /> : <Paperclip size={18} />}{t("common.file")}</button>
          <span>{t("chat.enterHint")}</span>
        </div>
        <div className="composer">
          <textarea value={messageText} onChange={(event) => setMessageText(event.target.value)} onKeyDown={(event) => {
            if (event.key === "Enter" && !event.shiftKey) { event.preventDefault(); if (!pending && busyKey !== "send-text") onSendText(); }
          }} placeholder={pending ? t("chat.placeholderOffline") : t("chat.placeholderTo", { name: friend.nickname })} disabled={pending} rows={1} />
          <button className="send-button" onClick={onSendText} disabled={pending || busyKey === "send-text" || !messageText.trim()} aria-label={t("common.sendMessage")}>{busyKey === "send-text" ? <LoaderCircle size={19} className="spin" /> : <Send size={19} />}</button>
        </div>
      </footer>
    </section>
  );
}

function MessageBubble({ message, transfer, busy, attachmentBusy, onRetry, onCancelTransfer, onOpenAttachment, onSaveAttachment, onRevealAttachment }: { message: ChatMessage; transfer?: TransferRecord; busy: boolean; attachmentBusy: boolean; onRetry: (messageId: string) => void; onCancelTransfer: (transferId: string) => void; onOpenAttachment: (message: ChatMessage) => void; onSaveAttachment: (message: ChatMessage) => void; onRevealAttachment: (message: ChatMessage) => void }) {
  const { t, formatTime } = useI18n();
  const outgoing = message.direction === "outgoing";
  const presentation = transfer ? transferStatusPresentation(transfer, transferStatusLabels(t)) : null;
  return (
    <div className={`message-line ${outgoing ? "outgoing" : "incoming"}`}>
      <div className={`message-bubble ${message.kind !== "text" ? "attachment-bubble" : ""}`}>
        {message.kind === "text" ? <p>{message.body}</p> : message.kind === "image" ? <ImageMessage message={message} busy={attachmentBusy} onOpen={onOpenAttachment} onSaveAs={onSaveAttachment} onReveal={onRevealAttachment} /> : <FileMessage message={message} busy={attachmentBusy} onOpen={onOpenAttachment} onSaveAs={onSaveAttachment} onReveal={onRevealAttachment} />}
        {transfer && <TransferStatusDisplay transfer={transfer} />}
        <div className="message-meta"><span>{formatTime(message.createdAt)}</span>{outgoing && <MessageStatusIcon status={message.status} />}</div>
        {message.status === "failed" && <div className="message-error"><AlertCircle size={13} /><span>{t("chat.sendFailed")}</span>{message.kind === "text" && <button disabled={busy} onClick={() => onRetry(message.messageId)}>{busy ? <LoaderCircle size={12} className="spin" /> : <RefreshCw size={12} />} {t("common.retry")}</button>}</div>}
        {transfer && presentation?.showCancel && <button type="button" className="cancel-link" disabled={busy} onClick={() => onCancelTransfer(transfer.transferId)}>{t("common.cancelTransfer")}</button>}
      </div>
    </div>
  );
}

function ImageMessage({ message, busy, onOpen, onSaveAs, onReveal }: AttachmentMessageProps) {
  const { t } = useI18n();
  const [preview, setPreview] = useState<string | null>(null);
  useEffect(() => {
    let active = true;
    if (message.status === "delivered") void invoke<string | null>("image_preview", { messageId: message.messageId }).then((value) => active && setPreview(value)).catch(() => undefined);
    return () => { active = false; };
  }, [message.messageId, message.status]);
  return <div className="image-message">{preview ? <img src={preview} alt={message.fileName || t("chat.imageAlt")} /> : <div className="image-placeholder"><ImageIcon size={30} /><span>{message.fileName}</span></div>}<AttachmentActions message={message} busy={busy} onOpen={onOpen} onSaveAs={onSaveAs} onReveal={onReveal} /></div>;
}

function FileMessage({ message, busy, onOpen, onSaveAs, onReveal }: AttachmentMessageProps) {
  const { t, formatBytes } = useI18n();
  return <div className="file-message"><div className="file-summary"><span className="file-icon"><File size={22} /></span><span><strong>{message.fileName || t("common.file")}</strong><small>{formatBytes(message.fileSize || 0)}</small></span></div><AttachmentActions message={message} busy={busy} onOpen={onOpen} onSaveAs={onSaveAs} onReveal={onReveal} /></div>;
}

interface AttachmentMessageProps {
  message: ChatMessage;
  busy: boolean;
  onOpen: (message: ChatMessage) => void;
  onSaveAs: (message: ChatMessage) => void;
  onReveal: (message: ChatMessage) => void;
}

function AttachmentActions({ message, busy, onOpen, onSaveAs, onReveal }: AttachmentMessageProps) {
  const { t } = useI18n();
  if (!attachmentActionState(message).available) return null;
  return (
    <div className="attachment-actions" role="group" aria-label={message.fileName || t("common.file")}>
      <button type="button" disabled={busy} onClick={() => onOpen(message)} title={t("common.open")}><ExternalLink size={14} /> <span>{t("common.open")}</span></button>
      <button type="button" disabled={busy} onClick={() => onSaveAs(message)} title={t("common.saveAs")}><FileDown size={14} /> <span>{t("common.saveAs")}</span></button>
      <button type="button" disabled={busy} onClick={() => onReveal(message)} title={t("common.showInFolder")}><FolderOpen size={14} /> <span>{t("common.showInFolder")}</span></button>
    </div>
  );
}

function TransferOfferCard({ transfer, busy, onResolve, onCancel }: { transfer: TransferRecord; busy: boolean; onResolve: (transfer: TransferRecord, accepted: boolean) => void; onCancel: (transferId: string) => void }) {
  const { t, formatBytes } = useI18n();
  const incoming = transfer.direction === "incoming";
  const presentation = transferStatusPresentation(transfer, transferStatusLabels(t));
  return <div className={`message-line ${incoming ? "incoming" : "outgoing"}`}><div className={`transfer-card ${presentation.tone}`}><span className="file-icon"><FileDown size={22} /></span><span><small>{incoming ? t("chat.receivedFile") : t("chat.sendingFile")}</small><strong>{transfer.fileName}</strong><em>{formatBytes(transfer.fileSize)}</em></span>{incoming && transfer.status === "awaitingAcceptance" ? <div className="transfer-actions"><button type="button" disabled={busy} onClick={() => onResolve(transfer, false)}>{t("common.reject")}</button><button type="button" className="primary" disabled={busy} onClick={() => onResolve(transfer, true)}>{busy ? <LoaderCircle size={14} className="spin" /> : <FileDown size={14} />} {t("common.receive")}</button></div> : <><TransferStatusDisplay transfer={transfer} />{presentation.showCancel && <button type="button" className="cancel-link" disabled={busy} onClick={() => onCancel(transfer.transferId)}>{t("common.cancelTransfer")}</button>}</>}</div></div>;
}

function TransferStatusDisplay({ transfer }: { transfer: TransferRecord }) {
  const { t } = useI18n();
  const presentation = transferStatusPresentation(transfer, transferStatusLabels(t));
  return <div className={`transfer-progress ${transfer.status} ${presentation.tone}`} role="status" aria-live={transfer.status === "paused" ? "polite" : undefined}>{presentation.showProgress && <div role="progressbar" aria-label={presentation.label} aria-valuemin={0} aria-valuemax={100} aria-valuenow={presentation.percent} aria-valuetext={presentation.label}><span style={{ width: `${presentation.percent}%` }} /></div>}<small>{presentation.label}</small></div>;
}

function MessageStatusIcon({ status }: { status: MessageStatus }) {
  if (status === "sending") return <LoaderCircle size={12} className="spin" />;
  if (status === "failed") return <AlertCircle size={12} className="status-error" />;
  return <CheckCheck size={13} />;
}

function SidebarSection({ title, count, icon, accent, headerAction, children }: { title: string; count: number; icon?: React.ReactNode; accent?: boolean; headerAction?: React.ReactNode; children: React.ReactNode }) {
  return <section className={`sidebar-section ${accent ? "accent" : ""}`}><header>{icon}<span>{title}</span><em>{count}</em>{headerAction}</header>{children}</section>;
}
function SidebarEmpty({ icon, text }: { icon: React.ReactNode; text: string }) { return <div className="sidebar-empty">{icon}<span>{text}</span></div>; }
function Avatar({ name, online, large }: { name: string; online: boolean; large?: boolean }) {
  const hue = [...name].reduce((sum, character) => sum + character.charCodeAt(0), 0) % 360;
  return <span className={`avatar ${large ? "large" : ""}`} style={{ "--avatar-hue": hue } as React.CSSProperties}>{name.trim().charAt(0).toLocaleUpperCase() || "L"}<i className={online ? "online" : "offline"} /></span>;
}

function WelcomePanel({ nearbyCount, friendCount }: { nearbyCount: number; friendCount: number }) {
  const { t } = useI18n();
  return <section className="welcome-panel"><div className="welcome-art"><span className="pulse pulse-one" /><span className="pulse pulse-two" /><span className="welcome-logo"><MessageCircleMore size={40} /></span></div><p className="eyebrow">{t("app.localPrivateFast")}</p><h1>{t("welcome.title")}</h1><p>{t("welcome.description")}</p><div className="welcome-stats"><span><Wifi size={18} /><strong>{nearbyCount}</strong><small>{t("welcome.nearbyOnline")}</small></span><span><UsersRound size={18} /><strong>{friendCount}</strong><small>{t("welcome.myFriends")}</small></span><span><ShieldCheck size={18} /><strong>{t("common.local")}</strong><small>{t("welcome.localTransfer")}</small></span></div></section>;
}

function Onboarding({ busy, languageBusy, onLanguageChange, onSubmit }: { busy: boolean; languageBusy: boolean; onLanguageChange: (preference: LanguagePreference) => Promise<void>; onSubmit: (nickname: string) => Promise<void> }) {
  const { t, preference, localeOptions } = useI18n();
  const [nickname, setNickname] = useState("");
  return <main className="onboarding-screen"><section className="onboarding-card"><div className="onboarding-copy"><span className="brand-mark large"><MessageCircleMore size={30} /></span><p className="eyebrow">{t("onboarding.eyebrow")}</p><h1>{t("onboarding.title")}</h1><p>{t("onboarding.description")}</p><ul><li><Wifi size={17} /> {t("onboarding.discovery")}</li><li><ShieldCheck size={17} /> {t("onboarding.identity")}</li><li><Paperclip size={17} /> {t("onboarding.transfer")}</li></ul></div><form className="nickname-form" onSubmit={(event) => { event.preventDefault(); if (nickname.trim()) void onSubmit(nickname); }}><span className="form-icon"><CircleUserRound size={25} /></span><h2>{t("onboarding.question")}</h2><p>{t("onboarding.nicknamePrivacy")}</p><label>{t("common.language")}<select value={preference} disabled={languageBusy} onChange={(event) => void onLanguageChange(event.target.value as LanguagePreference)}><option value="auto">{t("common.systemDefault")}</option>{localeOptions.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}</select></label><label>{t("onboarding.nickname")}<input autoFocus maxLength={32} value={nickname} onChange={(event) => setNickname(event.target.value)} placeholder={t("onboarding.nicknameExample")} /></label><button className="primary-button" disabled={busy || !nickname.trim()}>{busy ? <LoaderCircle size={18} className="spin" /> : <Wifi size={18} />}{busy ? t("onboarding.starting") : t("onboarding.enter")}</button><small><ShieldCheck size={13} /> {t("onboarding.noAccount")}</small></form></section></main>;
}

function ProfileDialog({ profile, languagePreference, languageBusy, onLanguageChange, transferPreferences, busy, directoryBusy, notificationBusy, notificationPermission, appVersion, updateChecking, updateStatus, onCheckUpdates, onEnableNotifications, onChooseDirectory, onOpenDirectory, onClose, onSave }: {
  profile: LocalProfile;
  languagePreference: LanguagePreference;
  languageBusy: boolean;
  onLanguageChange: (preference: LanguagePreference) => Promise<void>;
  transferPreferences: TransferPreferences;
  busy: boolean;
  directoryBusy: boolean;
  notificationBusy: boolean;
  notificationPermission: NotificationPermission;
  appVersion: string;
  updateChecking: boolean;
  updateStatus: string;
  onCheckUpdates: () => Promise<void>;
  onEnableNotifications: () => Promise<void>;
  onChooseDirectory: (currentDirectory: string) => Promise<string | null>;
  onOpenDirectory: (path: string) => Promise<void>;
  onClose: () => void;
  onSave: (nickname: string, transferPreferences: TransferPreferences) => Promise<void>;
}) {
  const { t, localeOptions } = useI18n();
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
        <button type="button" className="dialog-close" onClick={onClose} aria-label={t("common.close")}><X size={18} /></button>
        <Avatar name={nickname || profile.nickname} online large />
        <h2 id="profile-title">{t("settings.title")}</h2>
        <p>{t("settings.description")}</p>
        <label>{t("common.language")}<select value={languagePreference} disabled={languageBusy} onChange={(event) => void onLanguageChange(event.target.value as LanguagePreference)}><option value="auto">{t("common.systemDefault")}</option>{localeOptions.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}</select></label>
        <label>{t("onboarding.nickname")}<input autoFocus maxLength={32} value={nickname} onChange={(event) => setNickname(event.target.value)} /></label>
        <div className="device-id"><Monitor size={15} /><span><small>{t("settings.deviceIdentity")}</small><code>{shortPeerId(profile.peerId)}</code></span></div>
        <div className="auto-receive-setting">
          <FileDown size={17} />
          <span><strong>{t("settings.autoReceive")}</strong><small>{t("settings.autoReceiveDescription")}</small></span>
          <button type="button" className={autoReceiveFiles ? "enabled" : ""} role="switch" aria-checked={autoReceiveFiles} aria-label={t("settings.autoReceive")} onClick={() => setAutoReceiveFiles((enabled) => !enabled)}><i /></button>
        </div>
        <div className="receive-directory-setting">
          <FolderOpen size={17} />
          <span><strong>{t("settings.receiveLocation")}</strong><code title={receiveDirectory}>{receiveDirectory}</code></span>
          <div>
            <button type="button" disabled={!receiveDirectory || directoryBusy} onClick={() => void onOpenDirectory(receiveDirectory)} title={t("settings.openReceiveFolder")}>{directoryBusy ? t("common.opening") : t("common.open")}</button>
            <button type="button" onClick={() => void chooseDirectory()}>{t("common.change")}</button>
          </div>
        </div>
        <div className="notification-setting">
          <BellRing size={17} />
          <span><strong>{t("settings.notifications")}</strong><small>{t("settings.notificationsDescription")}</small></span>
          <button type="button" disabled={notificationBusy || notificationEnabled} onClick={() => void onEnableNotifications()}>
            {notificationBusy ? <LoaderCircle size={13} className="spin" /> : notificationEnabled ? <Check size={13} /> : null}
            {notificationEnabled ? t("settings.notificationsEnabled") : notificationPermission === "denied" ? t("settings.notificationsAuthorizeAgain") : t("settings.notificationsEnable")}
          </button>
        </div>
        <div className="update-setting">
          <RefreshCw size={17} />
          <span><strong>Weline Localnet {appVersion ? `v${appVersion}` : ""}</strong><small>{updateStatus}</small></span>
          <button type="button" disabled={updateChecking} onClick={() => void onCheckUpdates()}>
            {updateChecking && <LoaderCircle size={13} className="spin" />}{updateChecking ? t("settings.checkingUpdates") : t("settings.checkUpdates")}
          </button>
        </div>
        <div className="about-setting">
          <Building2 size={17} />
          <span><strong>{t("settings.company")}</strong><button type="button" onClick={() => void openUrl("mailto:contact@amayum.com")}><Mail size={12} /> contact@amayum.com</button></span>
        </div>
        <button className="primary-button" disabled={busy || !nickname.trim() || (autoReceiveFiles && !receiveDirectory.trim())}>{busy && <LoaderCircle size={16} className="spin" />} {t("settings.save")}</button>
      </form>
    </div>
  );
}

function BootScreen() { const { t } = useI18n(); return <main className="boot-screen"><section className="boot-card"><div className="boot-mark"><MessageCircleMore size={34} /></div><p className="eyebrow">{t("app.localPrivateFast")}</p><h1>Weline Localnet</h1><p>{t("boot.preparing")}</p><div className="loading-track"><span /></div></section></main>; }
function FatalScreen({ message, onRetry }: { message: string; onRetry: () => void }) { const { t } = useI18n(); return <main className="boot-screen"><section className="boot-card fatal-card"><div className="boot-mark error"><AlertCircle size={32} /></div><h1>{t("boot.failedTitle")}</h1><p>{message}</p><button className="primary-button" onClick={onRetry}><RefreshCw size={17} /> {t("common.retry")}</button></section></main>; }
function Toast({ toast, onClose }: { toast: ToastState; onClose: () => void }) { return <div className={`toast ${toast.tone}`} role="status">{toast.tone === "success" ? <Check size={17} /> : toast.tone === "error" ? <AlertCircle size={17} /> : <Wifi size={17} />}<span>{toast.message}</span><button onClick={onClose}><X size={14} /></button></div>; }

async function notifyIncomingFriendRequest(request: FriendRequest, t: Translator): Promise<void> {
  try {
    if (await getCurrentWindow().isFocused() || !(await isPermissionGranted())) return;
    sendNotification({
      title: "Weline Localnet",
      body: t("notifications.friendRequestBody", { name: request.nickname }),
    });
  } catch (error) {
    console.debug("Native friend-request notification unavailable", error);
  }
}
async function notifyIncomingTransfer(transfer: TransferRecord, t: Translator): Promise<void> {
  try {
    if (await getCurrentWindow().isFocused() || !(await isPermissionGranted())) return;
    sendNotification({
      title: "Weline Localnet",
      body: t("notifications.fileReceivedBody", { name: transfer.fileName }),
    });
  } catch (error) {
    console.debug("Native file-received notification unavailable", error);
  }
}
function platformLabel(platform: Platform, t: Translator): string { return platform === "windows" ? "Windows" : platform === "macos" ? "macOS" : t("common.desktopDevice"); }
function shortPeerId(peerId: string): string { return peerId.length > 20 ? `${peerId.slice(0, 9)}…${peerId.slice(-7)}` : peerId; }

type Translator = (key: TranslationKey, params?: Record<string, string | number>) => string;

function localizedError(error: unknown, t: Translator): string {
  if (error && typeof error === "object" && "code" in error && typeof error.code === "string") {
    const knownCodes = new Set([
      "invalid_input", "storage_error", "identity_error", "network_error", "permission_error",
      "update_error", "peer_offline", "not_friend", "incompatible_protocol", "integrity_failure",
      "destination_preflight_error", "io_error",
    ]);
    if (knownCodes.has(error.code)) return t(`errors.${error.code}` as TranslationKey);
  }
  return t("common.operationFailed");
}

function transferStatusLabels(t: Translator): TransferStatusLabels {
  return {
    awaitingAcceptanceIncoming: t("transfer.waitingConfirm"),
    awaitingAcceptanceOutgoing: t("transfer.waitingPeer"),
    transferring: (percent) => t("transfer.inProgress", { percent }),
    paused: t("transfer.paused"),
    destinationDirectoryUnavailable: t("transfer.destinationDirectoryUnavailable"),
    destinationPermissionDenied: t("transfer.destinationPermissionDenied"),
    destinationInsufficientSpace: t("transfer.destinationInsufficientSpace"),
    destinationFilesystemLimit: t("transfer.destinationFilesystemLimit"),
    destinationUnsupportedFilesystem: t("transfer.destinationUnsupportedFilesystem"),
    destinationFileTooLarge: t("transfer.destinationFileTooLarge"),
    completed: t("transfer.completed"),
    cancelled: t("transfer.cancelled"),
    failed: t("transfer.failed"),
  };
}

createRoot(document.getElementById("root")!).render(<StrictMode><I18nProvider><App /></I18nProvider></StrictMode>);
