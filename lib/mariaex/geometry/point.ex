defmodule Mariaex.Geometry.Point do
  @moduledoc """
  Define the Point struct
  """

  @type t :: %Mariaex.Geometry.Point{ coordinates: {number, number}, srid: non_neg_integer | nil }
  defstruct coordinates: {0, 0}, srid: nil
end
