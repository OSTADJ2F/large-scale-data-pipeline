# Large-Scale Data Pipeline

A production-style transportation analytics platform that ingests public
taxi-trip data, enriches it with historical weather data, and exposes
analytical dashboards and query APIs.

## Status

> ⚠️ Work in progress — repository scaffolding.

## Overview

This pipeline ingests NYC Yellow Taxi trip data and Open-Meteo historical
weather data, validates and normalizes the raw records, enriches them with
weather context, and builds analytical data marts. A Streamlit dashboard and a
FastAPI query API expose demand, revenue, route performance, and weather-impact
metrics.

The full architecture, setup instructions, and command reference will be
documented here as each phase lands.

## Quick start

See the README sections below as they are populated during development.

## License

[MIT](./LICENSE)
