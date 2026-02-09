# Power BI DAX Measures – Netflix Streaming Analytics

## KPI Card Measures

```dax
// Total Active Viewers (latest window)
Total Active Viewers =
CALCULATE(
    SUM(ViewerCount[active_viewers]),
    FILTER(
        ViewerCount,
        ViewerCount[window_end] = MAX(ViewerCount[window_end])
    )
)

// Total Unique Titles Being Watched
Active Titles =
CALCULATE(
    DISTINCTCOUNT(ViewerCount[content_id]),
    FILTER(
        ViewerCount,
        ViewerCount[window_end] = MAX(ViewerCount[window_end])
    )
)

// Average Watch Duration (minutes)
Avg Watch Duration (min) =
DIVIDE(
    AVERAGE(WatchTime[avg_watch_seconds]),
    60,
    0
)

// Completion Rate
Completion Rate % =
VAR TotalStarts =
    CALCULATE(COUNTROWS(ViewerCount), ViewerCount[total_events] > 0)
VAR TotalCompletes =
    CALCULATE(COUNTROWS(WatchTime), WatchTime[session_count] > 0)
RETURN
    DIVIDE(TotalCompletes, TotalStarts, 0) * 100

// Buffer Rate (buffers per 1000 events)
Buffer Rate Per 1K =
DIVIDE(
    SUM(BufferMetrics[buffer_count]),
    SUM(DeviceDistribution[event_count]),
    0
) * 1000

// Average Buffer Duration (seconds)
Avg Buffer Duration (s) =
DIVIDE(
    AVERAGE(BufferMetrics[avg_buffer_ms]),
    1000,
    0
)
```

## Trending Measures

```dax
// Trending Score with Rank
Trending Rank =
RANKX(
    ALL(TrendingContent[content_title]),
    [Trending Score Value],
    ,
    DESC,
    Dense
)

Trending Score Value =
SUM(TrendingContent[trending_score])

// Viewer Growth (compared to previous window)
Viewer Growth % =
VAR CurrentViewers = SUM(ViewerCount[active_viewers])
VAR PreviousViewers =
    CALCULATE(
        SUM(ViewerCount[active_viewers]),
        DATEADD(ViewerCount[window_end], -1, MINUTE)
    )
RETURN
    DIVIDE(CurrentViewers - PreviousViewers, PreviousViewers, 0) * 100
```

## Geographic Measures

```dax
// Top Country by Viewers
Top Country =
TOPN(
    1,
    SUMMARIZE(
        GeoDistribution,
        GeoDistribution[country],
        "Viewers", SUM(GeoDistribution[active_viewers])
    ),
    [Viewers],
    DESC
)

// Viewers by Region
Regional Viewers =
SUM(GeoDistribution[active_viewers])
```

## Device Measures

```dax
// Device Share %
Device Share % =
DIVIDE(
    SUM(DeviceDistribution[unique_users]),
    CALCULATE(
        SUM(DeviceDistribution[unique_users]),
        ALL(DeviceDistribution[device_type])
    ),
    0
) * 100

// Average Bitrate by Device
Avg Bitrate (Mbps) =
DIVIDE(
    AVERAGE(DeviceDistribution[avg_bitrate_kbps]),
    1000,
    0
)
```

## Engagement Measures

```dax
// Power Viewers Count
Power Viewers =
CALCULATE(
    COUNTROWS(EngagementScores),
    EngagementScores[engagement_segment] = "power_viewer"
)

// Average Engagement Score
Avg Engagement Score =
AVERAGE(EngagementScores[engagement_score])

// Engagement Distribution
Segment Distribution % =
DIVIDE(
    COUNTROWS(EngagementScores),
    CALCULATE(
        COUNTROWS(EngagementScores),
        ALL(EngagementScores[engagement_segment])
    ),
    0
) * 100
```

## Recommended Dashboard Layout

| Row | Left Panel | Center Panel | Right Panel |
|-----|-----------|-------------|-------------|
| 1   | KPI Cards (Active Viewers, Titles, Avg Duration) | Real-time Line Chart (viewers over time) | Trending Top 10 Bar Chart |
| 2   | World Map (geo distribution) | Device Donut Chart | Buffer Alert Table |
| 3   | Engagement Segment Pie | Watch Time Histogram | Completion Rate Gauge |
