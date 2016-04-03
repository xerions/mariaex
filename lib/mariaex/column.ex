defmodule Mariaex.Column do
  @moduledoc """
  Struct build for column definitions
  """

  defstruct name: nil,
            table: nil,
            type: nil,
            flags: nil
end
