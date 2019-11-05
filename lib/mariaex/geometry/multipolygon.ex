defmodule Mariaex.Geometry.MultiPolygon do
  @moduledoc """
  Define the MultiPolygon struct
  """

  @type t :: %Mariaex.Geometry.MultiPolygon{
          coordinates: [[{number, number}]],
          srid: non_neg_integer | nil
        }
  defstruct coordinates: [], srid: nil
end
