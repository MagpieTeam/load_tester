defmodule LoadTester do
  use Application

  def start(_type, _args) do
    LoadTester.Supervisor.start_link()
  end
end
