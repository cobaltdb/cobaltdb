import { usePageTitle } from '@hooks/usePageTitle'
import { HeroSection } from '@sections/HeroSection'
import { FeaturesSection } from '@sections/FeaturesSection'
import { ShowcaseSection } from '@sections/ShowcaseSection'
import { UseCasesSection } from '@sections/UseCasesSection'
import { ComparisonSection } from '@sections/ComparisonSection'
import { PerformanceSection } from '@sections/PerformanceSection'
import { ArchitectureSection } from '@sections/ArchitectureSection'
import { CodeExampleSection } from '@sections/CodeExampleSection'
import { TestimonialsSection } from '@sections/TestimonialsSection'
import { GetStartedSection } from '@sections/GetStartedSection'
import { TechStackSection } from '@sections/TechStackSection'
import { CTASection } from '@sections/CTASection'

export function HomePage() {
  usePageTitle()
  return (
    <div className="flex flex-col">
      <HeroSection />
      <FeaturesSection />
      <ShowcaseSection />
      <PerformanceSection />
      <ComparisonSection />
      <UseCasesSection />
      <ArchitectureSection />
      <TestimonialsSection />
      <CodeExampleSection />
      <GetStartedSection />
      <TechStackSection />
      <CTASection />
    </div>
  )
}
