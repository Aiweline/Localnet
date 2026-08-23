!define LOCALNET_FIREWALL_RULE "Localnet LAN Discovery"

!macro NSIS_HOOK_POSTINSTALL
  DetailPrint "Configuring Localnet LAN firewall access"
  nsExec::ExecToLog '"$SYSDIR\netsh.exe" advfirewall firewall delete rule name=$"${LOCALNET_FIREWALL_RULE}$" program=$"$INSTDIR\${MAINBINARYNAME}.exe$"'
  Pop $0
  nsExec::ExecToLog '"$SYSDIR\netsh.exe" advfirewall firewall add rule name=$"${LOCALNET_FIREWALL_RULE}$" dir=in action=allow program=$"$INSTDIR\${MAINBINARYNAME}.exe$" enable=yes profile=any remoteip=LocalSubnet'
  Pop $0
  ${If} $0 != 0
    DetailPrint "Unable to configure Localnet LAN firewall access (exit code $0)"
    MessageBox MB_ICONEXCLAMATION|MB_OK "Localnet 已安装，但 Windows 未能自动添加局域网防火墙规则。请在 Windows 安全中心允许 Localnet 访问本地网络。"
  ${EndIf}
!macroend

!macro NSIS_HOOK_PREUNINSTALL
  DetailPrint "Removing Localnet LAN firewall access"
  nsExec::ExecToLog '"$SYSDIR\netsh.exe" advfirewall firewall delete rule name=$"${LOCALNET_FIREWALL_RULE}$" program=$"$INSTDIR\${MAINBINARYNAME}.exe$"'
  Pop $0
!macroend
