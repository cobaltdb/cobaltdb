import { Moon, Sun } from "lucide-react"
import { Button } from "@/components/ui/button"
import { useTheme } from "@/components/theme-provider"

export function ThemeToggle() {
  const { theme, setTheme } = useTheme()

  const isDark = theme === "dark" || (theme === "system" && window.matchMedia("(prefers-color-scheme: dark)").matches)

  const toggle = () => setTheme(isDark ? "light" : "dark")

  return (
    <Button variant="ghost" size="icon" onClick={toggle} aria-label="Toggle theme">
      {isDark ? (
        <Sun className="h-[1.1rem] w-[1.1rem] transition-transform hover:rotate-45" />
      ) : (
        <Moon className="h-[1.1rem] w-[1.1rem] transition-transform hover:-rotate-12" />
      )}
    </Button>
  )
}
