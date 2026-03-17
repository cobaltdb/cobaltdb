# CobaltDB Website

A professional, comprehensive website for CobaltDB built with React 19, Tailwind CSS 3.4, and shadcn/ui.

## Features

- **Home Page**: Hero section, feature highlights, performance charts, code examples, CTA
- **Documentation**: Comprehensive SQL reference, API docs, guides
- **Examples**: Basic, advanced, real-world, and pattern examples
- **Playground**: Interactive SQL editor with WASM support (coming soon)

## Tech Stack

- React 19 with TypeScript
- Vite build tool
- Tailwind CSS 3.4
- shadcn/ui components
- React Router v6
- Lucide React icons
- Recharts for charts
- Framer Motion for animations

## Development

```bash
# Install dependencies
npm install

# Start development server
npm run dev

# Build for production
npm run build

# Preview production build
npm run preview
```

## Deployment

The website is automatically deployed to GitHub Pages via GitHub Actions when changes are pushed to the `main` branch.

### Manual Deployment

1. Push changes to `main` branch
2. GitHub Actions workflow will build and deploy automatically
3. Site will be available at `https://cobaltdb.dev`

### GitHub Pages Setup

1. Go to repository Settings > Pages
2. Source: GitHub Actions
3. Domain: `cobaltdb.dev`

## Project Structure

```
website/
├── .github/workflows/     # CI/CD workflows
├── public/               # Static assets
├── src/
│   ├── components/       # React components
│   │   ├── docs/         # Documentation components
│   │   ├── layout/       # Layout components (Header, Footer)
│   │   └── ui/           # shadcn/ui components
│   ├── data/             # Data files (docs, examples)
│   ├── pages/            # Page components
│   ├── sections/         # Home page sections
│   ├── App.tsx           # Main app component
│   ├── index.css         # Global styles
│   └── main.tsx          # Entry point
├── components.json       # shadcn/ui config
├── tailwind.config.js    # Tailwind config
├── tsconfig.json         # TypeScript config
└── vite.config.ts        # Vite config
```

## Adding New Documentation

1. Edit `src/data/docs.tsx`
2. Add new section to the `docsData` object
3. Update `DocsSidebar.tsx` to add navigation link

## License

MIT
