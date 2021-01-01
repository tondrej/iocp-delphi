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

unit ThreadPool;

{$IFDEF FPC}
  {$MODE Delphi}
{$ENDIF}

interface

uses
  Windows, Classes, SysUtils, SyncObjs,
  Compat;

type
  TWorkerProc = procedure (BytesTransferred: Cardinal; Overlapped: POverlapped; E: Exception);

  TThreadPool = class
  private
    FConcurrentThreadCount: Integer;
    FFinalized: TSimpleEvent;
    FHandle: THandle;
    FInitialized: TSimpleEvent;
    FRunningThreadCount: Integer;
    FThreadCount: Integer;

    FOnThreadDone: TNotifyEvent;
    FOnThreadStarted: TNotifyEvent;

    procedure ThreadDone(CompletionKey: NativeUInt);
    procedure ThreadStarted;
  protected
    function Broadcast(CompletionKey: UIntPtr; Overlapped: POverlapped = nil): Boolean;
    procedure DoThreadStarted; virtual;
    procedure DoThreadDone; virtual;
    procedure Finalize;
    procedure Initialize(AThreadCount: Cardinal = 0);

    property Handle: THandle read FHandle;
  public
    constructor Create(AThreadCount: Cardinal = 0; AConcurrentCount: Cardinal = 0);
    destructor Destroy; override;

    function BeginShutdown: Boolean;
    procedure Bind(Worker: TWorkerProc; AHandle: THandle = INVALID_HANDLE_VALUE);
    procedure Queue(BytesTransferred: Cardinal; Worker: TWorkerProc; Overlapped: POverlapped = nil);
    procedure Shutdown;
    procedure WaitFor(Timeout: Cardinal);

    property ConcurrentThreadCount: Integer read FConcurrentThreadCount;
    property RunningThreadCount: Integer read FRunningThreadCount;
    property ThreadCount: Integer read FThreadCount;

    property OnThreadDone: TNotifyEvent read FOnThreadDone write FOnThreadDone;
    property OnThreadStarted: TNotifyEvent read FOnThreadStarted write FOnThreadStarted;
  end;

implementation

const
  CompletionKeyShutDown = 0;

function ThreadPoolWorker(ThreadPool: TThreadPool): Cardinal; stdcall;
var
  BytesTransferred: Cardinal;
  CompletionKey: NativeUInt;
  Worker: TWorkerProc absolute CompletionKey;
  Overlapped: POverlapped;
  E: Exception;
begin
  Result := 0;
  ThreadPool.ThreadStarted;
  try
    repeat
      BytesTransferred := 0;
      try
        if not GetQueuedCompletionStatus(ThreadPool.Handle, BytesTransferred, 
          CompletionKey, Overlapped, INFINITE) then
          RaiseLastOSError;

        case CompletionKey of
          CompletionKeyShutDown:
            Break;
          else
            Worker(BytesTransferred, Overlapped, nil);
        end;
      except
        if CompletionKey <> 0 then
        begin
          E := AcquireExceptionObject;
          Worker(BytesTransferred, Overlapped, E);
        end;
      end;
    until False;
  finally
    ThreadPool.ThreadDone(CompletionKey);
  end;
end;

{ TThreadPool private }

procedure TThreadPool.ThreadDone(CompletionKey: NativeUInt);
begin
  DoThreadDone;

  if InterlockedDecrement(FRunningThreadCount) <= 0 then
  begin
    FRunningThreadCount := 0;
    if CompletionKey = CompletionKeyShutDown then
      FFinalized.SetEvent;
  end;
end;

procedure TThreadPool.ThreadStarted;
begin
  DoThreadStarted;

  if InterlockedIncrement(FRunningThreadCount) >= FThreadCount then
    FInitialized.SetEvent;
end;

{ TThreadPool protected }

function TThreadPool.Broadcast(CompletionKey: UIntPtr; Overlapped: POverlapped): Boolean;
var
  Count: Cardinal;
  I: Integer;
begin
  Count := FRunningThreadCount;
  Result := Count > 0;
  if not Result then
    Exit;

  for I := 0 to Count - 1 do
    PostQueuedCompletionStatus(FHandle, 0, CompletionKey, Overlapped);
end;

procedure TThreadPool.DoThreadDone;
begin
  if Assigned(FOnThreadDone) then
    FOnThreadDone(Self);
end;

procedure TThreadPool.DoThreadStarted;
begin
  if Assigned(FOnThreadStarted) then
    FOnThreadStarted(Self);
end;

procedure TThreadPool.Finalize;
begin
  Shutdown;
end;

procedure TThreadPool.Initialize(AThreadCount: Cardinal);
var
  SystemInfo: TSystemInfo;
  I: Integer;
  ThreadHandle: THandle;
  ThreadId: Cardinal;
begin
  FInitialized.ResetEvent;
  FThreadCount := AThreadCount;
  if FThreadCount = 0 then
  begin
    GetSystemInfo(SystemInfo);
    FThreadCount := SystemInfo.dwNumberOfProcessors * 2;
    if FConcurrentThreadCount = 0 then
      FConcurrentThreadCount := SystemInfo.dwNumberOfProcessors;
  end;
  try
    for I := 0 to FThreadCount - 1 do
    begin
      ThreadHandle := CreateThread(nil, 0, @ThreadPoolWorker, Self, 0, ThreadId);
      if ThreadHandle = 0 then
        RaiseLastOSError;
    end;
    FInitialized.WaitFor(INFINITE);
  except
    Finalize;
    raise;
  end;
end;

{ TThreadPool public }

constructor TThreadPool.Create(AThreadCount, AConcurrentCount: Cardinal);
begin
  inherited Create;
  FHandle := 0;
  FThreadCount := AThreadCount;
  FConcurrentThreadCount := AConcurrentCount;
  FInitialized := TSimpleEvent.Create;
  FFinalized := TSimpleEvent.Create;
end;

destructor TThreadPool.Destroy;
begin
  Shutdown;
  FFinalized.Free;
  FInitialized.Free;
  if FHandle <> 0 then
    CloseHandle(FHandle);
  inherited Destroy;
end;

function TThreadPool.BeginShutdown: Boolean;
begin
  Result := Broadcast(CompletionKeyShutDown);
end;

procedure TThreadPool.Bind(Worker: TWorkerProc; AHandle: THandle);
var
  NewHandle: Boolean;
  CompletionKey: NativeUInt absolute Worker;
begin
  NewHandle := FHandle = 0;
  FHandle := CreateIoCompletionPort(AHandle, FHandle, CompletionKey, FConcurrentThreadCount);
  if FHandle = 0 then
    RaiseLastOSError;
  if NewHandle then
    Initialize(FThreadCount);
end;

procedure TThreadPool.Queue(BytesTransferred: Cardinal; Worker: TWorkerProc; Overlapped: POverlapped);
var
  CompletionKey: NativeUint absolute Worker;
begin
  if FHandle = 0 then
    Bind(Worker);

  Win32Check(PostQueuedCompletionStatus(FHandle, BytesTransferred, CompletionKey, Overlapped));
end;

procedure TThreadPool.Shutdown;
begin
  if BeginShutdown then
    WaitFor(INFINITE);
end;

procedure TThreadPool.WaitFor(Timeout: Cardinal);
begin
  FFinalized.WaitFor(Timeout);
end;

initialization
  IsMultiThread := True;

finalization

end.
