ExUnit.configure exclude: [:ssl_tests]
ExUnit.start()

run_cmd = fn cmd ->
  key = :ecto_setup_cmd_output
  Process.put(key, "")
  status = Mix.Shell.cmd(cmd, fn(data) ->
    current = Process.get(key)
    Process.put(key, current <> data)
  end)
  output = Process.get(key)
  Process.put(key, "")
  {status, output}
end

mysql_pass_switch = if mysql_root_pass = System.get_env("MYSQL_ROOT_PASSWORD") do
  "-p#{mysql_root_pass}"
else
  ""
end

mysql_port = System.get_env("MDBPORT") || 3306
mysql_host = System.get_env("MDBHOST") || "localhost"
mysql_protocol = System.get_env("MDBPROTOCOL") || "tcp"
mysql_connect = "-u root #{mysql_pass_switch} --host=#{mysql_host} --port=#{mysql_port} --protocol=#{mysql_protocol}"

sql = """
  CREATE TABLE test1 (id serial, title text);
  INSERT INTO test1 VALUES(1, 'test');
  INSERT INTO test1 VALUES(2, 'test2');
  DROP TABLE test1;
"""

cmds = if System.get_env("MYSQL_5_7") do
  [
    ~s(mysql #{mysql_connect} -e "DROP USER 'mariaex_user'@'localhost';"),
    ~s(mysql #{mysql_connect} -e "CREATE USER 'mariaex_user'@'localhost' IDENTIFIED BY 'mariaex_pass';"),
    ~s(mysql #{mysql_connect} -e "GRANT ALL PRIVILEGES ON *.* TO 'mariaex_user'@'localhost' WITH GRANT OPTION;"),
    ~s(mysql #{mysql_connect} -e "DROP DATABASE IF EXISTS mariaex_test;"),
    ~s(mysql #{mysql_connect} -e "CREATE DATABASE mariaex_test DEFAULT CHARACTER SET 'utf8' COLLATE 'utf8_general_ci'";),
    ~s(mysql #{mysql_connect} mariaex_test -e "#{sql}"),
    ~s(mysql --host=#{mysql_host} --port=#{mysql_port} --protocol=#{mysql_protocol} -u mariaex_user -pmariaex_pass mariaex_test -e "#{sql}")
  ]
else
  [
    ~s(mysql #{mysql_connect} -e "GRANT ALL ON *.* TO 'mariaex_user'@'localhost' IDENTIFIED BY 'mariaex_pass';"),
    ~s(mysql #{mysql_connect} -e "DROP DATABASE IF EXISTS mariaex_test;"),
    ~s(mysql #{mysql_connect} -e "CREATE DATABASE mariaex_test DEFAULT CHARACTER SET 'utf8' COLLATE 'utf8_general_ci'";),
    ~s(mysql #{mysql_connect} mariaex_test -e "#{sql}"),
    ~s(mysql --host=#{mysql_host} --port=#{mysql_port} --protocol=#{mysql_protocol} -u mariaex_user -pmariaex_pass mariaex_test -e "#{sql}")
  ]
end

Enum.each(cmds, fn cmd ->
  {status, output} = run_cmd.(cmd)
  IO.puts "--> #{output}"

  if status != 0 do
    IO.puts """
    Command:
    #{cmd}
    error'd with:
    #{output}
    Please verify the user "root" exists and it has permissions to
    create databases and users.
    If the "root" user requires a password, set the environment
    variable MYSQL_ROOT_PASSWORD to its value.
    Beware that the password may be visible in the process list!
    """
    System.halt(1)
  end
end)

defmodule Mariaex.TestHelper do
  defmacro query(stat, params, opts \\ []) do
    quote do
      case Mariaex.Connection.query(var!(context)[:pid], unquote(stat),
                                     unquote(params), unquote(opts)) do
        {:ok, %Mariaex.Result{rows: nil}} -> :ok
        {:ok, %Mariaex.Result{rows: rows}} -> rows
        {:error, %Mariaex.Error{} = err} -> err
      end
    end
  end

  defmacro execute_text(stat, params, opts \\ []) do
    quote do
      case Mariaex.query(var!(context)[:pid], unquote(stat),
            unquote(params), [query_type: :text] ++ unquote(opts)) do
        {:ok, %Mariaex.Result{rows: nil}} -> :ok
        {:ok, %Mariaex.Result{rows: rows}} -> rows
        {:error, %Mariaex.Error{} = err} -> err
      end
    end
  end

  defmacro with_prepare!(name, stat, params, opts \\ []) do
    quote do
      conn = var!(context)[:pid]
      query = Mariaex.prepare!(conn, unquote(name), unquote(stat), unquote(opts))
      case Mariaex.execute!(conn, query, unquote(params)) do
        %Mariaex.Result{rows: nil} -> :ok
        %Mariaex.Result{rows: rows} -> rows
      end
    end
  end

  defmacro prepare(stat, opts \\ []) do
    quote do
      case Mariaex.prepare(var!(context)[:pid], unquote(stat), unquote(opts)) do
        {:ok, %Mariaex.Query{} = query} -> query
        {:error, %Mariaex.Error{} = err} -> err
      end
    end
  end

  defmacro execute(query, params, opts \\ []) do
    quote do
      case Mariaex.execute(var!(context)[:pid], unquote(query), unquote(params),
                           unquote(opts)) do
        {:ok, %Mariaex.Result{rows: nil}} -> :ok
        {:ok, %Mariaex.Result{rows: rows}} -> rows
        {:error, %Mariaex.Error{} = err} -> err
      end
    end
  end

  defmacro close(query, opts \\ []) do
    quote do
      case Mariaex.close(var!(context)[:pid], unquote(query), unquote(opts)) do
        :ok -> :ok
        {:error, %Mariaex.Error{} = err} -> err
      end
    end
  end

  def capture_log(fun) do
    Logger.remove_backend(:console)
    fun.()
    Logger.add_backend(:console, flush: true)
  end

  def length_encode_row(row) do
    Enum.map_join(row, &(<<String.length(&1)>> <> &1))
  end

end
