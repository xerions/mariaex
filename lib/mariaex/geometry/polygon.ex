defmodule Mariaex.Geometry.Polygon do
  @moduledoc """
  Define the Polygon struct
  """

  @type t :: %Mariaex.Geometry.Polygon{ coordinates: [[{number, number}]], srid: non_neg_integer | nil }
  defstruct coordinates: [], srid: nil
end
