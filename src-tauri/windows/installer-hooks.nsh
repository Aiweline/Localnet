; Tauri's NSIS template caches the previous installer language in HKCU.
; Always show the requested ten-language selector for interactive installs.
!define MUI_LANGDLL_ALWAYSSHOW

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
  StrCpy $R9 "en-US"
  StrCmp $LANGUAGE ${LANG_SIMPCHINESE} 0 +2
    StrCpy $R9 "zh-CN"
  StrCmp $LANGUAGE ${LANG_SPANISH} 0 +2
    StrCpy $R9 "es-ES"
  StrCmp $LANGUAGE ${LANG_FRENCH} 0 +2
    StrCpy $R9 "fr-FR"
  StrCmp $LANGUAGE ${LANG_GERMAN} 0 +2
    StrCpy $R9 "de-DE"
  StrCmp $LANGUAGE ${LANG_PORTUGUESEBR} 0 +2
    StrCpy $R9 "pt-BR"
  StrCmp $LANGUAGE ${LANG_RUSSIAN} 0 +2
    StrCpy $R9 "ru-RU"
  StrCmp $LANGUAGE ${LANG_JAPANESE} 0 +2
    StrCpy $R9 "ja-JP"
  StrCmp $LANGUAGE ${LANG_KOREAN} 0 +2
    StrCpy $R9 "ko-KR"
  StrCmp $LANGUAGE ${LANG_ARABIC} 0 +2
    StrCpy $R9 "ar-SA"

  SetShellVarContext current
  CreateDirectory "$APPDATA\com.aiweline.localnet"
  System::Call 'ole32::CoCreateGuid(g .s)'
  Pop $R7
  StrCmp $R7 "" weline_localnet_locale_done
  ClearErrors
  FileOpen $R8 "$APPDATA\com.aiweline.localnet\installer-locale" w
  IfErrors weline_localnet_locale_done
  FileWrite $R8 "$R9$\n$R7"
  FileClose $R8
weline_localnet_locale_done:
!macroend
