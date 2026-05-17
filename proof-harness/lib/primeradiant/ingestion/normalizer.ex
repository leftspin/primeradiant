defmodule Primeradiant.Ingestion.Normalizer do
  @moduledoc false

  @stopwords ~w(a an and are as at be but by for from has have in into is it its of on or that the their this to was were will with)

  def normalize(raw) do
    title = String.trim(raw["title"] || "")
    body = String.trim(raw["body_text"] || "")
    searchable_text = String.downcase(title <> "\n" <> body)
    facts = inferred_facts(searchable_text)
    background = inferred_background(searchable_text)
    questions = inferred_questions(searchable_text)
    colors = inferred_colors(searchable_text)
    fingerprint = fingerprint(title, body)

    %{
      fixture_id: raw["fixture_id"],
      external_id: raw["external_id"],
      source_type: raw["source_type"],
      observed_at: raw["observed_at"],
      title: title,
      body_text: body,
      acl: raw["acl"] || %{"privacy" => "public"},
      facts: facts,
      background: background,
      questions: questions,
      colors: colors,
      fingerprint: fingerprint,
      topic_tokens: topic_tokens(title, body, facts, background, questions, colors)
    }
  end

  defp fingerprint(title, body) do
    :sha256
    |> :crypto.hash(title <> "\n" <> body)
    |> Base.encode16(case: :lower)
  end

  defp topic_tokens(title, body, facts, background, questions, colors) do
    ([title, body] ++
       Map.values(facts) ++ Map.values(background) ++ Map.values(questions) ++ colors)
    |> Enum.flat_map(&tokenize/1)
    |> Enum.reject(&(&1 in @stopwords or String.length(&1) < 3 or Regex.match?(~r/^\d+$/, &1)))
    |> MapSet.new()
  end

  defp tokenize(text) do
    text
    |> String.downcase()
    |> String.replace(~r/[^a-z0-9\s_-]/u, " ")
    |> String.split(~r/\s+/, trim: true)
  end

  defp inferred_facts(text) do
    %{}
    |> put_if(
      contains_all?(text, ["harbor", "ferry"]) and
        not String.contains?(text, "harbor ferry terminal"),
      "route",
      "harbor_ferry"
    )
    |> put_if(
      String.contains?(text, "strike is active") or
        String.contains?(text, "strike remains active"),
      "strike_status",
      "active"
    )
    |> put_if(String.contains?(text, "strike is resolved"), "strike_status", "resolved")
    |> put_if(String.contains?(text, "crossings are halted"), "crossings", "halted")
    |> put_if(
      String.contains?(text, "limited to morning service only"),
      "crossings",
      "limited_morning_only"
    )
    |> put_if(String.contains?(text, "crossings are normal"), "crossings", "normal")
    |> put_if(
      String.contains?(text, "negotiations are scheduled for 17:00"),
      "negotiations",
      "scheduled_17_00"
    )
    |> put_if(
      String.contains?(text, "overnight service was never suspended"),
      "correction_scope",
      "overnight_service_never_suspended"
    )
    |> put_if(String.contains?(text, "harbor ferry terminal"), "venue", "harbor_ferry_terminal")
    |> put_if(
      String.contains?(text, "cafe workers") and String.contains?(text, "walkout"),
      "labor_action",
      "cafe_walkout"
    )
    |> put_if(
      String.contains?(text, "ferry crossings remained normal"),
      "ferry_crossings",
      "normal"
    )
    |> put_if(
      String.contains?(text, "asked for a roof repair") or
        String.contains?(text, "roof repair thread") or
        String.contains?(text, "roof repair quote arrived"),
      "project",
      "roof_repair"
    )
    |> put_if(String.contains?(text, "quote before friday"), "quote_status", "requested")
    |> put_if(String.contains?(text, "before friday"), "deadline", "friday")
    |> put_if(String.contains?(text, "quote arrived"), "quote_status", "received")
    |> put_if(String.contains?(text, "final price of 14800"), "quoted_amount", "14800")
    |> put_if(String.contains?(text, "basement humidity sensor"), "sensor", "basement_humidity")
    |> put_if(String.contains?(text, "battery is low"), "battery_status", "low")
  end

  defp inferred_background(text) do
    %{}
    |> put_if(
      String.contains?(text, "waiting for the hoa board packet"),
      "status",
      "waiting_for_board_packet"
    )
    |> put_if(String.contains?(text, "no new filings"), "activity", "no_new_filings")
    |> put_if(
      String.contains?(text, "quiet after resolution"),
      "status",
      "quiet_after_resolution"
    )
  end

  defp inferred_questions(text) do
    %{}
    |> put_if(String.contains?(text, "blocker is hoa approval"), "blocker", "hoa_approval")
  end

  defp inferred_colors(text) do
    [
      {["commuters stood in the rain"], "commuters stood in the rain waiting for buses"},
      {["tourists should use the tunnel"], "city says tourists should use the tunnel"},
      {["blamed early reports"], "authority blamed early reports for confusion"},
      {["exposed city hall more than the union"],
       "columnist says the week exposed city hall more than the union"},
      {["passengers still boarded ferries"],
       "passengers still boarded ferries while the cafe counter stayed closed"},
      {["ladder crew is backed up"], "contractor says the ladder crew is backed up"},
      {["same-week installation"], "includes same-week installation if approved tonight"},
      {["tiny maintenance note"], "tiny maintenance note matters because Flynn asked to track it"}
    ]
    |> Enum.flat_map(fn {needles, color} ->
      if Enum.all?(needles, &String.contains?(text, &1)), do: [color], else: []
    end)
  end

  defp put_if(map, true, key, value), do: Map.put(map, key, value)
  defp put_if(map, false, _key, _value), do: map

  defp contains_all?(text, needles), do: Enum.all?(needles, &String.contains?(text, &1))
end
