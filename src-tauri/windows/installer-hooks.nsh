!macro NSIS_HOOK_PREINSTALL
  ReadRegStr $R8 HKCU "Software\aiweline\Weline Chat" ""
  StrCmp $R8 "" weline_localnet_preinstall_done
  StrCpy $INSTDIR $R8
  SetOutPath $INSTDIR
weline_localnet_preinstall_done:
!macroend

!macro NSIS_HOOK_POSTINSTALL
  ReadRegStr $R8 HKCU "Software\aiweline\Weline Chat" ""
  StrCmp $R8 "" weline_localnet_postinstall_done

  SetShellVarContext current
  Delete "$SMPROGRAMS\Weline Chat.lnk"
  Delete "$DESKTOP\Weline Chat.lnk"
  DeleteRegKey HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\Weline Chat"
  DeleteRegKey HKCU "Software\aiweline\Weline Chat"
  RMDir "$LOCALAPPDATA\Weline Localnet"

weline_localnet_postinstall_done:
!macroend
