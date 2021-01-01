(*

MIT License

Copyright (c) 2021 Ondrej Kelle

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

  InstanceCount = 128;
  MaxBufferSize = 1024;

type
  TOperation = (opConnect, opRead, opWrite);

  PPipeInfo = ^TPipeInfo;
  TPipeInfo = record
    Overlapped: TOverlapped;
    Handle: THandle;
    Operation: TOperation;
    Buffer: array[0..MaxBufferSize - 1] of AnsiChar;
  end;

  TInfoProc = procedure (Info: PPipeInfo; BytesTransferred: Cardinal = 0; Callback: Boolean = False; E: Exception = nil);

const
  OpStrings: array[TOperation, Boolean] of string = (
    ('connecting', 'connected'),
    ('reading', 'read'),
    ('writing', 'written')
  );
  PipeName = '\\.\pipe\ASYNCPIPE';

var
  Pool: TThreadPool;
  ServerList: TThreadList;
  InfoProc: TInfoProc = nil;

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

procedure RequestConnect(HandlerProc: TWorkerProc);
var
  Info: PPipeInfo;
  LastError: Cardinal;
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
  except
    FreeMem(Info);
    raise;
  end;
end;

function RequestRead(Info: PPipeInfo): Boolean;
var
  BytesRead, LastError: Cardinal;
begin
  Info^.Operation := opRead;
  if Assigned(InfoProc) then
    InfoProc(Info);
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

function RequestWrite(Info: PPipeInfo): Boolean;
var
  BytesWritten, LastError: Cardinal;
begin
  Info^.Operation := opWrite;
  if Assigned(InfoProc) then
    InfoProc(Info);
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
  if Assigned(InfoProc) then
    InfoProc(Info, BytesTransferred, True, E);

  if Assigned(E) then
  begin
    E.Free;
    Disconnect(Info);
    RequestConnect(Server_Handler);
    Exit;
  end;

  case Info^.Operation of
    opConnect:
      RequestRead(Info);
    opRead:
      begin
{$ifdef HAS_ANSISTRINGS}
        AnsiStrings.StrLCopy(Info^.Buffer, PAnsiChar(AnsiStrings.UpperCase(Info^.Buffer)), MaxBufferSize - 1);
{$else}
        StrLCopy(Info^.Buffer, PAnsiChar(UpperCase(Info^.Buffer)), MaxBufferSize - 1);
{$endif}
        RequestWrite(Info);
      end;
    opWrite:
      RequestRead(Info);
  end;
end;

procedure ServerInfoProc(Info: PPipeInfo; BytesTransferred: Cardinal; Callback: Boolean; E: Exception);
var
  SBytes, SData: string;
begin
  SBytes := '';
  SData := '';
  if Callback then
    SBytes := Format(' (%u bytes)', [BytesTransferred]);

  if Assigned(E) then
    WriteLn(Format('[%s][%u] server %u %s: [%s] %s', [FormatDateTime('hh:nn:ss.zzz', Now),
      GetCurrentThreadId, Info^.Handle, OpStrings[Info^.Operation, Callback] + SBytes, E.ClassName, E.Message]))
  else
  begin
    if Callback or (Info^.Operation = opWrite) then
      SData := Format(' ''%s''', [PAnsiChar(Info^.Buffer)]);

    case Info^.Operation of
      opWrite, opRead:
        WriteLn(Format('[%s][%u] server %u %s' + SData, [FormatDateTime('hh:nn:ss.zzz', Now),
          GetCurrentThreadId, Info^.Handle, OpStrings[Info^.Operation, Callback] + SBytes, PAnsiChar(Info^.Buffer)]));
      else
        WriteLn(Format('[%s][%u] server %u %s', [FormatDateTime('hh:nn:ss.zzz', Now),
          GetCurrentThreadId, Info^.Handle, OpStrings[Info^.Operation, Callback]]));
    end;
  end;
end;

procedure ServerMain;
var
  I: Integer;
begin
  InfoProc := ServerInfoProc;
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

procedure NewRequest(Info: PPipeInfo);
var
  SHello: AnsiString;
begin
  SHello := Format('Hello %d', [Random(101)]);
  {$ifdef HAS_ANSISTRINGS}AnsiStrings.{$endif}StrLCopy(Info^.Buffer, PAnsiChar(SHello), MaxBufferSize - 1);
end;

procedure Client_Handler(BytesTransferred: Cardinal; Overlapped: POverlapped; E: Exception);
var
  Info: PPipeInfo absolute Overlapped;
begin
  if Assigned(InfoProc) then
    InfoProc(Info, BytesTransferred, True, E);

  if Assigned(E) then
  begin
    E.Free;
    Exit;
  end;

  case Info^.Operation of
    opRead:
      begin
        NewRequest(Info);
        RequestWrite(Info);
      end;
    opWrite:
      RequestRead(Info);
  end;
end;

procedure ClientInfoProc(Info: PPipeInfo; BytesTransferred: Cardinal; Callback: Boolean; E: Exception);
var
  SBytes, SData: string;
begin
  SBytes := '';
  SData := '';
  if Callback then
    SBytes := Format(' (%u bytes)', [BytesTransferred]);

  if Assigned(E) then
    WriteLn(Format('[%s][%u] client %u %s: [%s] %s', [FormatDateTime('hh:nn:ss.zzz', Now), GetCurrentThreadId,
      Info^.Handle, OpStrings[Info^.Operation, Callback] + SBytes, E.ClassName, E.Message]))
  else
  begin
    if Callback or (Info^.Operation = opWrite) then
      SData := Format(' ''%s''', [PAnsiChar(Info^.Buffer)]);

    WriteLn(Format('[%s][%u] client %u %s' + SData, [FormatDateTime('hh:nn:ss.zzz', Now), GetCurrentThreadId,
      Info^.Handle, OpStrings[Info^.Operation, Callback] + SBytes]));
  end;
end;

procedure ClientMain;
var
  ClientHandle: THandle;
  Info: PPipeInfo;
  PipeState: Cardinal;
begin
  Randomize;
  InfoProc := ClientInfoProc;

  Info := nil;
  Pool := nil;
  try
    Pool := TThreadPool.Create;
    ClientHandle := CreateFile(PipeName, GENERIC_READ or GENERIC_WRITE, 0, nil, OPEN_EXISTING,
      FILE_FLAG_OVERLAPPED, 0);
    if ClientHandle = INVALID_HANDLE_VALUE then
      RaiseLastOSError;
    try
      PipeState := PIPE_READMODE_MESSAGE;
      Win32Check(SetNamedPipeHandleState(ClientHandle, PipeState, nil, nil));

      Pool.Bind(Client_Handler, ClientHandle);

      Info := AllocMem(SizeOf(TPipeInfo));
      Info^.Handle := ClientHandle;
      Info^.Operation := opWrite;

      NewRequest(Info);
      RequestWrite(Info);
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
