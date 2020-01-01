(*

MIT License

Copyright (c) 2020 Ondrej Kelle

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

program workerdemo;

{$IFDEF FPC}
  {$MODE Delphi}
{$ENDIF}

{$APPTYPE CONSOLE}

uses
  Windows,
  SysUtils,
  SyncObjs,
  ThreadPool in '..\..\src\ThreadPool.pas',
  Compat in '..\..\src\Compat.pas';

var
  Lock: TCriticalSection;

procedure Writeln(const S: string);
begin
  Lock.Acquire;
  try
    System.Writeln(S);
  finally
    Lock.Release;
  end;
end;

var
  PendingCount: Integer;
  Cancelled: Boolean;

function Fibonacci(Index: Int64): Int64;
begin
  if Cancelled then
    Result := 0
  else if Index < 3 then
    Result := 1
  else
    Result := Fibonacci(Index - 1) + Fibonacci(Index - 2);
end;

procedure WorkerProc(BytesTransferred: Cardinal; Overlapped: POverlapped; E: Exception);
var
  I, F: Int64;
  TaskID: NativeUInt absolute Overlapped;
begin
  if Assigned(E) then
  begin
    Writeln(Format('Task %u: [%s] %s', [TaskID, E.ClassName, E.Message]));
    E.Free;
    Exit;
  end;

  I := Random(42);
  F := Fibonacci(I);
  if Cancelled then
    Writeln(Format('Task %u: [%u] cancelled', [TaskID, GetCurrentThreadId]))
  else
    Writeln(Format('Task %u: [%u] Fibonacci(%d) = %d', [TaskID, GetCurrentThreadId, I, F]));
  InterlockedDecrement(PendingCount);
end;

procedure Main;
const
  TaskCount = 1024;
var
  ThreadPool: TThreadPool;
  I: NativeUInt;
begin
  Randomize;
  ThreadPool := TThreadPool.Create(64, 32); // optional: override the thread count defaults
  try
    PendingCount := TaskCount;
    ThreadPool.Bind(WorkerProc); // optional: initialize beforehand so you can see the actual thread counts
    Writeln(Format('Posting %d tasks to a pool with %d running threads (%d concurrent). Press Enter to cancel...',
      [TaskCount, ThreadPool.RunningThreadCount, ThreadPool.ConcurrentThreadCount]));
    for I := 0 to TaskCount - 1 do
      ThreadPool.Queue(0, WorkerProc, Pointer(I));
    Readln;
    if PendingCount > 0 then
    begin
      Writeln(Format('Cancelling %u pending tasks...', [PendingCount]));
      Cancelled := True;
    end;
  finally
    ThreadPool.Free;
  end;
end;

begin
  try
    Lock := TCriticalSection.Create;
    try
      Main;
    finally
      Lock.Free;
    end;
  except
    on E: Exception do
    begin
      ExitCode := 1;
      Writeln(Format('[%s] %s', [E.ClassName, E.Message]));
    end;
  end;
end.
