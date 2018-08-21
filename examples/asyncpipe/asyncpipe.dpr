(*

MIT License

Copyright (c) 2018 Ondrej Kelle

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

*)

program asyncpipe;

{$i common.inc}

{$APPTYPE CONSOLE}

uses
  Windows,
  Types,
  Classes,
  SysUtils,
{$ifdef HAS_ANSISTRINGS}
  AnsiStrings,
{$endif}
  ThreadPool in '..\..\src\ThreadPool.pas',
  Compat in '..\..\src\Compat.pas';

const
  PIPE_REJECT_REMOTE_CLIENTS = 8;
  PIPE_UNLIMITED_INSTANCES = 255;

  InstanceCount = 4;
  MaxBufferSize = 4096;

type
  TOperation = (opConnect, opRead, opWrite);

  PPipeInfo = ^TPipeInfo;
  TPipeInfo = record
    Overlapped: TOverlapped;
    Handle: THandle;
    Operation: TOperation;
    Buffer: array[0..MaxBufferSize - 1] of AnsiChar;
  end;

const
  OpStrings: array[TOperation] of AnsiString = ('connect', 'read', 'write');
  PipeName = '\\.\pipe\ASYNCPIPE';

var
  Pool: TThreadPool;
  ServerList: TThreadList;

procedure Server_Handler(BytesTransferred: Cardinal; Overlapped: POverlapped; E: Exception); forward;

procedure Disconnect(Info: PPipeInfo);
begin
  with ServerList.LockList do
  try
    Remove(Info);
  finally
    ServerList.UnlockList;
  end;
  CloseHandle(Info^.Handle);
  FreeMem(Info);
end;

procedure RequestConnect(Info: PPipeInfo; HandlerProc: TWorkerProc); overload;
var
  LastError: Cardinal;
begin
  Info^.Operation := opConnect;
  if ConnectNamedPipe(Info^.Handle, Pointer(Info)) then // connected synchronously
    // nothing to do, callback invoked by IOCP
  else
  begin
    LastError := GetLastError;
    case LastError of
      ERROR_PIPE_CONNECTED, ERROR_PIPE_LISTENING, ERROR_IO_PENDING:
        ;
      else
        RaiseLastOSError(LastError);
    end;
  end;
end;

procedure RequestConnect(HandlerProc: TWorkerProc); overload;
var
  Info: PPipeInfo;
begin
  Info := AllocMem(SizeOf(TPipeInfo));
  try
    Info^.Handle := CreateNamedPipe(PipeName, PIPE_ACCESS_DUPLEX or FILE_FLAG_OVERLAPPED,
      PIPE_TYPE_MESSAGE or PIPE_READMODE_MESSAGE or PIPE_WAIT,
      InstanceCount, MaxBufferSize, MaxBufferSize, 0, nil);
    if Info^.Handle = INVALID_HANDLE_VALUE then
      RaiseLastOSError;

    Pool.Bind(Server_Handler, Info^.Handle);
    with ServerList.LockList do
      try
        Add(Info);
      finally
        ServerList.UnlockList;
      end;
    RequestConnect(Info, HandlerProc);
  except
    FreeMem(Info);
    raise;
  end;
end;

function RequestRead(Info: PPipeInfo; HandlerProc: TWorkerProc): Boolean;
var
  BytesRead, LastError: Cardinal;
begin
  Info^.Operation := opRead;
  BytesRead := 0;
  Result := ReadFile(Info^.Handle, Info^.Buffer, MaxBufferSize, BytesRead, Pointer(Info));
  if Result then // read completed synchronously
    // nothing to do, callback invoked by IOCP
  else
  begin
    LastError := GetLastError;
    if LastError <> ERROR_IO_PENDING then
      RaiseLastOSError(LastError);
  end;
end;

function RequestWrite(Info: PPipeInfo; HandlerProc: TWorkerProc): Boolean;
var
  BytesWritten, LastError: Cardinal;
begin
  Info^.Operation := opWrite;
  BytesWritten := 0;
  Result := WriteFile(Info^.Handle, Info^.Buffer, {$ifdef HAS_ANSISTRINGS}AnsiStrings.{$endif}StrLen(Info^.Buffer) + 1,
    BytesWritten, Pointer(Info));
  if Result then // write completed synchronously
    // nothing to do, callback invoked by IOCP
  else
  begin
    LastError := GetLastError;
    if LastError <> ERROR_IO_PENDING then
      RaiseLastOSError(LastError);
  end;
end;

procedure Server_Handler(BytesTransferred: Cardinal; Overlapped: POverlapped; E: Exception);
var
  Info: PPipeInfo absolute Overlapped;
begin
  if Assigned(E) then
  begin
    OutputDebugString(PChar(Format('Server %u %s [%s] %s', [Info^.Handle, OpStrings[Info^.Operation], E.ClassName, E.Message])));
    E.Free;
    Disconnect(Info);
    RequestConnect(Server_Handler);
    Exit;
  end;

  case Info^.Operation of
    opConnect:
      OutputDebugString(PChar(Format('Server %u %s', [Info^.Handle, OpStrings[Info^.Operation]])));
    else
      OutputDebugString(PChar(Format('Server %u %s %u bytes: ''%s''', [Info^.Handle, OpStrings[Info^.Operation],
        BytesTransferred, Info^.Buffer])));
  end;

  case Info^.Operation of
    opConnect:
      RequestRead(Info, Server_Handler);
    opRead:
      begin
{$ifdef HAS_ANSISTRINGS}
        AnsiStrings.StrLCopy(Info^.Buffer, PAnsiChar(AnsiStrings.UpperCase(Info^.Buffer)), MaxBufferSize - 1);
{$else}
        StrLCopy(Info^.Buffer, PAnsiChar(UpperCase(Info^.Buffer)), MaxBufferSize - 1);
{$endif}
        RequestWrite(Info, Server_Handler);
      end;
    opWrite:
      RequestRead(Info, Server_Handler);
  end;
end;

procedure Client_Handler(BytesTransferred: Cardinal; Overlapped: POverlapped; E: Exception);
var
  Info: PPipeInfo absolute Overlapped;
begin
  OutputDebugString(PChar(Format('[%u] %u bytes %s', [GetCurrentThreadId, BytesTransferred, OpStrings[Info^.Operation]])));
  if Assigned(E) then
  begin
    Writeln(Format('Client %u %s [%s] %s', [Info^.Handle, OpStrings[Info^.Operation], E.ClassName, E.Message]));
    E.Free;
    Exit;
  end;

  Writeln(Format('[%u] Client %u %s %u bytes: ''%s''', [GetCurrentThreadId, Info^.Handle, OpStrings[Info^.Operation], BytesTransferred,
    Info^.Buffer]));

  case Info^.Operation of
    opRead:
      begin
{$ifdef HAS_ANSISTRINGS}
        AnsiStrings.StrLCopy(Info^.Buffer, PAnsiChar(AnsiString(Format('%s Hello',
          [FormatDateTime('hh:nn:ss.zzz', Now)]))), MaxBufferSize - 1);
{$else}
        StrLCopy(Info^.Buffer, PAnsiChar(AnsiString(Format('%s Hello',
          [FormatDateTime('hh:nn:ss.zzz', Now)]))), MaxBufferSize - 1);
{$endif}
        RequestWrite(Info, Client_Handler);
      end;
    opWrite:
      RequestRead(Info, Client_Handler);
  end;
end;

procedure ServerMain;
var
  I: Integer;
begin
  ServerList := nil;
  Pool := TThreadPool.Create;
  try
    ServerList := TThreadList.Create;
    ServerList.LockList;
    try
      for I := 0 to InstanceCount - 1 do
        RequestConnect(Server_Handler);
    finally
      ServerList.UnlockList;
    end;

    Writeln(Format('%d instances listening. Press Enter to quit...', [InstanceCount]));
    Readln;
  finally
    Pool.Free;
    if Assigned(ServerList) then
    begin
      with ServerList.LockList do
        try
          for I := 0 to Count - 1 do
          begin
            CloseHandle(PPipeInfo(Items[I])^.Handle);
            FreeMem(Items[I]);
          end;
          Clear;
        finally
          ServerList.UnlockList;
        end;
      ServerList.Free;
    end;
  end;
end;

procedure ClientMain;
var
  ClientHandle: THandle;
  Info: PPipeInfo;
  PipeState: Cardinal;
begin
  Info := nil;
  Pool := nil;
  try
    Pool := TThreadPool.Create;
    ClientHandle := CreateFile(PipeName, GENERIC_READ or GENERIC_WRITE, 0, nil, OPEN_EXISTING,
      FILE_FLAG_OVERLAPPED, 0);
    if ClientHandle = INVALID_HANDLE_VALUE then
      RaiseLastOSError;
    try
      Writeln(Format('Client: %u', [NativeInt(ClientHandle)]));

      PipeState := PIPE_READMODE_MESSAGE;
      Win32Check(SetNamedPipeHandleState(ClientHandle, PipeState, nil, nil));

      Pool.Bind(Client_Handler, ClientHandle);

      Info := AllocMem(SizeOf(TPipeInfo));
      Info^.Handle := ClientHandle;
      Info^.Operation := opWrite;

{$ifdef HAS_ANSISTRINGS}
      AnsiStrings.StrLCopy(Info^.Buffer, PAnsiChar(AnsiString(Format('%s Hello',
        [FormatDateTime('hh:nn:ss.zzz', Now)]))), MaxBufferSize - 1);
{$else}
        StrLCopy(Info^.Buffer, PAnsiChar(AnsiString(Format('%s Hello',
          [FormatDateTime('hh:nn:ss.zzz', Now)]))), MaxBufferSize - 1);
{$endif}
      RequestWrite(Info, Client_Handler);
      Readln;
    finally
      CloseHandle(ClientHandle);
    end;
  finally
    Pool.Free;
    if Assigned(Info) then
      FreeMem(Info);
  end;
end;

procedure Main;
begin
  if (ParamCount = 1) and SameText('/server', ParamStr(1)) then
    ServerMain
  else
    ClientMain;
end;

begin
  try
    Main;
  except
    on E: Exception do
    begin
      ExitCode := 1;
      Writeln(Format('[%s] %s', [E.ClassName, E.Message]));
    end;
  end;
end.
