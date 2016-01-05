defmodule LoadTester.Worker do
  use GenServer
  use Timex
  require Logger

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, opts)
  end

  def init(:ok) do
    loggers = System.get_env("LOGGERS") |> String.to_integer
    :erlang.send_after(1000, self(), {:execute, loggers})
    
    {:ok, %{}}
  end

  def handle_info({:execute, loggers}, state) do
    execute(loggers)

    {:noreply, state}
  end

  def execute(loggers) do
    get_logger_ids()
    |> Enum.take(loggers)
    |> Enum.each(fn(id) -> start_logging(id) end)
  end

  def start_logging(logger_id) do
    spawn(fn() -> 
      sensors = Magpie.DataAccess.Sensor.get(logger_id)
      logger_id = Poison.encode!(%{logger_id: logger_id})
      case get_token(logger_id, 0) do
        :error -> :error
        {ip, token} -> do_log(ip, logger_id, token, sensors)
      end
    end)
  end

  def get_ip(seed_ip) do
    response = HTTPoison.get!("http://#{seed_ip}/api/nodes", ["Content-Type": "application/json"])
    nodes = Poison.decode!(response.body)
    :random.seed(:erlang.phash2([node()]),
            :erlang.monotonic_time(),
            :erlang.unique_integer())
    node = Enum.random(nodes)
    node["ip"]
  end

  def get_token(logger_id, attempts) do
    seed_ip = System.get_env("SEEDIP")
    ip = get_ip(seed_ip)

    case HTTPoison.post("http://#{ip}/api/start", logger_id, ["Content-Type": "application/json"]) do
      {:ok, %HTTPoison.Response{status_code: 200} = response} ->
        Logger.debug("GOTTOKEN | #{logger_id}")
        token = Poison.decode!(response.body)["token"]
        {ip, token}
      result when attempts < 25 ->
        Logger.debug("NOTOKEN | #{logger_id} | attempts: #{attempts} | #{inspect result}")
        5000 + (10000 * :rand.uniform()) |> trunc() |> :timer.sleep()
        get_token(logger_id, attempts + 1)
      result -> 
        Logger.debug("NEVERTOKEN | #{logger_id} | #{inspect result}")
        :error
    end
  end

  def do_log(ip, logger_id, token, sensors) do
    measurements = Enum.reduce(sensors, [], fn(s, acc) ->
      timestamp = Date.now |> DateFormat.format!("{s-epoch}") |> String.to_integer() |> Kernel.*(1000) |> to_string
      value = :rand.uniform() |> to_string
      metadata = "AAAF"
      [%{sensor_id: s[:id], timestamp: timestamp, value: value, metadata: metadata} | acc]
    end)
    log = Poison.encode!(%{
      token: token,
      measurements: measurements
    })

    {time, response} = Timex.Time.measure(fn() -> send_log(ip, log) end)
    case response do
      {:ok, _response} ->
        time = Time.to_msecs(time)
        Logger.debug("LOGOK | #{logger_id} | Time: #{time}")
      {:error, msg} -> 
        Logger.debug("LOGERROR | #{logger_id} | #{inspect msg}")
    end
    
    :timer.sleep(1000)

    do_log(ip, logger_id, token, sensors)
  end

  def send_log(ip, log) do
    HTTPoison.post("http://#{ip}/api/log", log, ["Content-Type": "application/json"])
  end

  def get_logger_ids() do
    Magpie.DataAccess.Logger.get()
    |> Stream.map(fn(logger) -> logger[:id] end)
  end
end