defmodule Mix.Tasks.LoadTester do
  use Mix.Task
  
  def run(_args) do
    IO.puts("Hi")
    LoadTester.execute()
    IO.puts("Bye!")
  end
  
end