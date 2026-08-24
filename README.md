# Weline Localnet

**Windows 与 macOS 局域网私密聊天、图片和文件直传工具。**<br>
**Private peer-to-peer LAN messaging and file transfer for Windows and macOS.**

[简体中文](#简体中文) · [English](#english) · [Español](#español) · [Français](#français) · [Deutsch](#deutsch) · [Português](#português-brasil) · [Русский](#русский) · [日本語](#日本語) · [한국어](#한국어) · [العربية](#العربية)

> 当前版本：`0.2.0`。README 提供 10 种语言的产品简介；Windows 安装器已配置对应的 10 种安装语言。

## 简体中文

Weline Localnet 是一款面向公司、工作室和家庭内网的桌面通信工具。设备位于同一局域网时，应用会自动发现附近的 Weline Localnet 用户；双方确认好友关系后，即可直接发送文字、图片和文件，不需要云端消息或文件中转服务器。

### 核心能力

| 能力 | 说明 |
| --- | --- |
| 自动发现 | 通过 mDNS、兼容 mDNS 和局域网信标发现 Windows/macOS 设备，并针对常见 TUN/代理环境使用多路径探测。 |
| 好友确认 | 附近设备不能直接发送内容，必须先发起好友申请并由接收方接受。 |
| 内网直传 | 文字、图片和文件通过设备间加密连接直接传输，不上传到 Weline 服务器。 |
| 可恢复大文件传输 | 双方均为 v0.2.0 时，v2 默认支持单个文件最高 100 GiB；使用 4 MiB 分块、每块 SHA-256 校验和已确认进度，在可恢复的断网或应用重启后自动续传。 |
| 自动接收 | 可在设置中开启；默认关闭，仅接收已添加好友的文件，并可指定保存目录。 |
| 安全落盘 | 清理异常文件名、防止目录穿越、校验 SHA-256；同名文件自动编号且不覆盖已有文件。 |
| 跨平台 | 支持 Windows 10/11 和 macOS 12 及以上版本，macOS 包同时覆盖 Apple Silicon 与 Intel。 |

### 自动接收文件

打开“设置”后，可以：

1. 选择文件接收目录；默认位置为系统“下载/Weline Localnet”。
2. 开启或关闭“自动接收好友文件”。
3. 直接打开当前接收目录。

自动接收默认关闭。开启后也只接受已经添加为好友的设备；陌生设备仍然无法发送文件。接收请求到达时，如果目录已被删除、不可写或暂时不可用，本次文件会回到手动确认流程。传输过程中不会覆盖已有文件；如果已开始传输后磁盘被拔出或权限被撤销，界面会明确显示失败，可恢复目录后重新发送。

### 网络与隐私

- 消息和文件只在本地网络设备之间传输。
- 设备身份保存在本机，并使用 Noise 认证的加密连接。
- v2 传输使用 4 MiB 分块，并对每个分块进行 SHA-256 完整性校验；已确认进度会保留以支持续传。
- 接收和续传前会检查可用磁盘空间、目标目录是否可写且可用，以及 FAT32/MSDOS 等文件系统的单文件大小限制。
- 双方均为 v0.2.0 时，单个文件默认最高 100 GiB，且 v2 不再有 2 GiB 硬限制。0.1.x 设备仍通过旧版 v1 兼容，单个文件最大为 2 GiB。
- 可恢复的网络中断或应用重启后，v2 传输会在条件恢复时自动继续，而不是从头开始。
- 不要求账号、手机号或互联网连接。
- Windows 防火墙和 macOS“本地网络”授权由操作系统管理；应用不会反复自动弹出授权窗口。

### 技术栈

- 桌面运行时：Tauri 2
- 网络与加密：Rust、libp2p、Noise、Yamux
- 界面：React 19、TypeScript、Vite
- 本地数据：SQLite 与系统安全凭据存储
- 支持平台：Windows x64、macOS Universal（arm64 + x86_64）

### 从源码运行

需要 Node.js 24、pnpm 10.28.2、Rust 1.85 或更高版本，以及对应平台的 Tauri 构建依赖。

```bash
pnpm install --frozen-lockfile
pnpm tauri dev
```

生成安装包：

```bash
pnpm tauri build
```

macOS Universal DMG 由仓库中的 GitHub Actions 工作流构建和校验。

## English

Weline Localnet is a private desktop messenger for offices, studios, and home networks. It discovers other Weline Localnet devices on the same LAN. After a friend request is approved, users can exchange text, images, and files directly between Windows and macOS devices without a cloud relay.

For v0.2.0 peers on both sides, resumable v2 transfers support a single file up to 100 GiB by default, without a 2 GiB protocol hard limit. Each 4 MiB chunk has SHA-256 integrity validation, and confirmed transfer state is retained so a transfer can continue automatically after recoverable network loss or an app restart instead of starting over. Before accepting or resuming, Localnet checks free disk space, destination writability and availability, and single-file limits on FAT32/MSDOS and similar file systems.

Version 0.1.x peers remain supported through legacy v1 with its existing 2 GiB per-file maximum; resumable v2 requires v0.2.0 on both peers. Automatic file receiving is optional and disabled by default. It works only for accepted friends, uses a user-selected folder, sanitizes unsafe names, numbers duplicates, and never overwrites an existing file. Multi-path discovery is designed for common TUN and proxy configurations while keeping all transfer traffic on the local network.

## Español

Weline Localnet es una aplicación privada para Windows y macOS que descubre otros dispositivos en la misma red local. Después de aceptar una solicitud de amistad, los usuarios pueden enviar mensajes, imágenes y archivos directamente, sin un servidor intermediario en la nube.

La recepción automática es opcional y está desactivada de forma predeterminada. Solo acepta archivos de amigos confirmados, permite elegir la carpeta de destino y nunca sobrescribe archivos existentes. El descubrimiento utiliza varias rutas para funcionar mejor en redes con TUN o proxy.

## Français

Weline Localnet est une application privée pour Windows et macOS qui découvre les autres appareils présents sur le même réseau local. Une fois la demande d’ami acceptée, les utilisateurs peuvent échanger directement des messages, des images et des fichiers, sans relais dans le cloud.

La réception automatique est facultative et désactivée par défaut. Elle s’applique uniquement aux amis confirmés, permet de choisir le dossier de destination et n’écrase jamais un fichier existant. La découverte multichemin améliore la compatibilité avec les environnements TUN et proxy courants.

## Deutsch

Weline Localnet ist eine private Desktop-App für Windows und macOS. Sie findet andere Weline-Localnet-Geräte im selben lokalen Netzwerk. Nach Bestätigung einer Freundschaftsanfrage können Textnachrichten, Bilder und Dateien direkt zwischen den Geräten übertragen werden – ohne Cloud-Zwischenserver.

Der automatische Dateiempfang ist optional und standardmäßig deaktiviert. Er gilt nur für bestätigte Freunde, verwendet einen frei wählbaren Zielordner und überschreibt keine vorhandenen Dateien. Die Mehrwege-Erkennung verbessert die Funktion in üblichen TUN- und Proxy-Umgebungen.

## Português (Brasil)

O Weline Localnet é um aplicativo privado para Windows e macOS que encontra outros dispositivos na mesma rede local. Após a aprovação de um pedido de amizade, os usuários podem trocar mensagens, imagens e arquivos diretamente, sem um servidor de retransmissão na nuvem.

O recebimento automático é opcional e vem desativado por padrão. Ele aceita arquivos somente de amigos confirmados, permite escolher a pasta de destino e nunca substitui arquivos existentes. A descoberta por múltiplos caminhos melhora a compatibilidade com ambientes TUN e proxy comuns.

## Русский

Weline Localnet — приватное приложение для Windows и macOS, которое обнаруживает другие устройства в той же локальной сети. После подтверждения запроса в друзья пользователи могут напрямую обмениваться текстом, изображениями и файлами без облачного сервера-посредника.

Автоматический приём файлов является необязательным и по умолчанию отключён. Он работает только для подтверждённых друзей, позволяет выбрать папку сохранения и не перезаписывает существующие файлы. Многоканальное обнаружение повышает совместимость с распространёнными конфигурациями TUN и прокси.

## 日本語

Weline Localnet は、同じローカルネットワーク上の Windows/macOS 端末を自動検出するプライベート通信アプリです。友だち申請を承認した後、クラウド中継サーバーを使わずに、メッセージ・画像・ファイルを端末間で直接送受信できます。

ファイルの自動受信は任意で、初期状態では無効です。承認済みの友だちからのファイルだけを受信し、保存先フォルダーを選択でき、既存ファイルを上書きしません。複数経路の検出により、一般的な TUN やプロキシ環境にも対応しやすくしています。

## 한국어

Weline Localnet은 같은 로컬 네트워크에 있는 Windows 및 macOS 장치를 자동으로 찾는 개인용 데스크톱 메신저입니다. 친구 요청을 승인한 뒤에는 클라우드 중계 서버 없이 메시지, 이미지, 파일을 장치 간에 직접 전송할 수 있습니다.

파일 자동 수신은 선택 기능이며 기본적으로 꺼져 있습니다. 승인된 친구의 파일만 수신하고 저장 폴더를 지정할 수 있으며 기존 파일을 덮어쓰지 않습니다. 다중 경로 검색은 일반적인 TUN 및 프록시 환경에서의 호환성을 높입니다.

## العربية

Weline Localnet هو تطبيق خاص لنظامي Windows وmacOS يكتشف الأجهزة الأخرى الموجودة على الشبكة المحلية نفسها. بعد قبول طلب الصداقة، يمكن للمستخدمين تبادل الرسائل والصور والملفات مباشرة بين الأجهزة من دون خادم وسيط سحابي.

الاستلام التلقائي للملفات اختياري ومعطّل افتراضياً. يعمل فقط مع الأصدقاء المقبولين، ويسمح باختيار مجلد الحفظ، ولا يستبدل الملفات الموجودة. يساعد الاكتشاف متعدد المسارات على العمل بصورة أفضل في بيئات TUN والوكيل الشائعة.

## 公司与联系 / Company & Contact

**成都阿玛云科技有限公司**<br>
Email: [contact@amayum.com](mailto:contact@amayum.com)

产品问题、安全问题或商务合作，请通过以上邮箱联系。
