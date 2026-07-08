defmodule Primeradiant.SoupCadenceSchedulerTest do
  use ExUnit.Case, async: true

  @script Path.expand("scripts/r1/run_soup_cadence_once.sh")
  @service Path.expand("deploy/eurisko/primeradiant-soup-cadence.service")
  @timer Path.expand("deploy/eurisko/primeradiant-soup-cadence.timer")
  @runbook Path.expand("../specs/primeradiant-eurisko-soup-api-deploy-runbook.md")

  test "scheduler wrapper invokes only the recurring soup cadence mix task" do
    script = File.read!(@script)

    assert script =~ "mix primeradiant.soup_cadence_once"
    assert script =~ "--cadence \"$CADENCE\""
    assert script =~ "--soup-db \"$PRIMERADIANT_SOUP_API_SOUP_DB\""
    assert script =~ "--tenant \"$PRIMERADIANT_SOUP_API_TENANT\""
    assert script =~ "active_story_transform_detect_link_15m"
    refute script =~ "soup_story_backfill_once"
    refute script =~ "admitted-soup-story-backfill"
    refute script =~ "news-morning"
    refute script =~ "Magazine"
  end

  test "systemd templates install a one-shot 15 minute Prime Radiant cadence timer" do
    service = File.read!(@service)
    timer = File.read!(@timer)

    assert service =~ "Type=oneshot"
    assert service =~ "run_soup_cadence_once.sh"
    assert service =~ "PRIMERADIANT_SOUP_CADENCE_CADENCE=active_story_transform_detect_link_15m"
    assert service =~ "PRIMERADIANT_SOUP_CADENCE_LIMIT=8"
    refute service =~ "soup_story_backfill_once"
    refute service =~ "news-morning"
    refute service =~ "Magazine"

    assert timer =~ "OnBootSec=10min"
    assert timer =~ "OnUnitActiveSec=15min"
    assert timer =~ "Persistent=true"
    assert timer =~ "Unit=primeradiant-soup-cadence.service"
  end

  test "runbook pins scheduler proof to Prime Radiant cadence, not product backfills" do
    runbook = File.read!(@runbook)

    assert runbook =~ "T1589 is the reviewed source-owned scheduler"
    assert runbook =~ "source_behavior` is `recurring_cadence_over_admitted_soup"
    assert runbook =~ "source_admission_performed` is `false"
    assert runbook =~ "must not invoke `primeradiant.soup_story_backfill_once`"
    assert runbook =~ "News/Reporter\n  `news-morning`"
  end
end
