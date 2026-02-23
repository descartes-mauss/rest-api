# AGENTS.md

## Stack

- **Framework:** React (v18+)
- **Language:** TypeScript (strict mode)
- **Build Tool:** Vite
- **Styling:** TailwindCSS
- **Linting:** ESLint (ECMA 2020 target)

## Core Principles

- Prefer functional components and hooks.
- Enforce strict typing (`noImplicitAny`, `strictNullChecks`).
- Keep components small and composable.
- Co-locate tests, styles, and types with components.
- Favor readability over clever abstractions.
- Prefer using `apiClient.ts` over custom requests implementations.
- Do not update classes under `components/` and `interfaces/`, sub-class new classes and categorize under `/ai/...`

## Project Structure

```shell
public/ # public assets like images bundled with the dist
src/ # main source folder
  |- client/ # external API clients, powered by axios
  |- components/ # main components folder
  |- context/ # context classes for shared state
  |- external/ # utility classes for interacting with external systems
  |- hooks/ # custom hooks for the project
  |- icons/ # SVG and TSX files representing all icons in the project
  |- interfaces/ # domain-based models (interfaces) and custom reusable types
  |- layout/ # layout classes that represent the visual aspects of the project
  |- pages/ # main views/states displayed by the routes
  |- providers/ # view content providers
  |- redux/ # state management classes
  |- App.tsx # main routing and displaying class
```

## React Rules

- Use `FC` only when children typing is required.
- Extract reusable logic into custom hooks.
- Memoize only when profiling shows need.
- Prefer context or composition over prop-drilling.
- No console logs
- No TODO comments unless explicitly requested
- Report errors via `Honeybadger`, do not silence.

## TypeScript Rules

- Use `unknown` if necessary but strictly do not use `any`.
- Explicit return types for exported functions.
- Use discriminated unions for state machines.
- Centralize shared types in `/interfaces`.
- Use utility types (`Partial`, `Pick`, `Record`, etc.) appropriately.

## TailwindCSS Rules

- Utility-first approach.
- Suggest extracting repeated patterns into components.
- Avoid inline styles unless dynamic.
- Refer to `src/index.css` for current rules.

## ESLint Guidelines

- Extend recommended + TypeScript rules.
- Follow `eslint.config.js`.
- Enforce import order and no-unused-vars.

## Vite Guidelines

- Use path aliases (`@/`).
- Enable fast refresh.
- Keep plugins minimal.
