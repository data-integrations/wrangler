'use client';

import type { Metadata } from 'next';
import { Inter } from 'next/font/google';
import '../styles/globals.css';
import Link from 'next/link';
import { useRouter } from 'next/navigation';

const inter = Inter({ subsets: ['latin'] });

// Metadata needs to be exported from a server component, so this won't work in a client component
// Moving this to a separate file or removing it for now
const metadata = {
  title: 'ZeoTap Data Ingestion Tool',
  description: 'Bidirectional ClickHouse & Flat File Data Ingestion Tool',
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  const router = useRouter();

  // Function to handle home button click - will navigate to the home page
  const handleHomeClick = () => {
    router.push('/');
    router.refresh(); // Force a refresh to reset state
  };

  return (
    <html lang="en">
      <head>
        <title>{metadata.title}</title>
        <meta name="description" content={metadata.description} />
      </head>
      <body className={inter.className}>
        <div className="min-h-screen bg-gray-50">
          <header className="bg-gradient-to-r from-primary to-accent text-white shadow-lg">
            <div className="container mx-auto px-4">
              <div className="flex items-center justify-between py-4">
                <div className="flex items-center space-x-3">
                  <div className="flex-shrink-0 w-10 h-10 bg-white rounded-full flex items-center justify-center">
                    <svg className="w-6 h-6 text-primary" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                      <path d="M12 19L21 12L12 5V19Z" fill="currentColor" />
                      <path d="M3 19L12 12L3 5V19Z" fill="currentColor" opacity="0.5" />
                    </svg>
                  </div>
                  <div>
                    <h1 className="text-2xl font-bold tracking-tight">ZeoTap</h1>
                    <p className="text-xs font-medium text-blue-100">Data Ingestion Tool</p>
                  </div>
                </div>
                <div className="hidden md:flex items-center space-x-6">
                  <span className="text-sm font-medium text-blue-100">Bidirectional ClickHouse & Flat File Data Ingestion</span>
                  <button 
                    onClick={handleHomeClick}
                    className="px-4 py-2 bg-white text-primary rounded-md text-sm font-medium hover:bg-blue-50 transition-colors flex items-center"
                  >
                    <svg className="w-4 h-4 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M3 12l2-2m0 0l7-7 7 7m-7-7v14"></path>
                    </svg>
                    Home
                  </button>
                </div>
                <div className="md:hidden">
                  <button
                    onClick={handleHomeClick}
                    className="text-white hover:text-blue-100 flex items-center"
                  >
                    <svg className="w-5 h-5 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M3 12l2-2m0 0l7-7 7 7m-7-7v14"></path>
                    </svg>
                    Home
                  </button>
                </div>
              </div>
            </div>
          </header>
          <main className="container mx-auto py-8 px-4">{children}</main>
          <footer className="bg-secondary text-white p-4 mt-8">
            <div className="container mx-auto">
              <div className="flex flex-col md:flex-row justify-between items-center">
                <div className="flex items-center space-x-2 mb-4 md:mb-0">
                  <div className="w-8 h-8 bg-white rounded-full flex items-center justify-center">
                    <svg className="w-4 h-4 text-secondary" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                      <path d="M12 19L21 12L12 5V19Z" fill="currentColor" />
                      <path d="M3 19L12 12L3 5V19Z" fill="currentColor" opacity="0.5" />
                    </svg>
                  </div>
                  <span className="font-medium">ZeoTap</span>
                </div>
                <p className="text-sm">&copy; {new Date().getFullYear()} ZeoTap | Data Ingestion Tool</p>
                <div className="flex space-x-4 mt-4 md:mt-0">
                  <a href="https://github.com/harshit1634" target="_blank" rel="noopener noreferrer" className="text-white hover:text-blue-100 transition-colors">
                    <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 24 24" aria-hidden="true">
                      <path fillRule="evenodd" d="M12 2C6.477 2 2 6.484 2 12.017c0 4.425 2.865 8.18 6.839 9.504.5.092.682-.217.682-.483 0-.237-.008-.868-.013-1.703-2.782.605-3.369-1.343-3.369-1.343-.454-1.158-1.11-1.466-1.11-1.466-.908-.62.069-.608.069-.608 1.003.07 1.531 1.032 1.531 1.032.892 1.53 2.341 1.088 2.91.832.092-.647.35-1.088.636-1.338-2.22-.253-4.555-1.113-4.555-4.951 0-1.093.39-1.988 1.029-2.688-.103-.253-.446-1.272.098-2.65 0 0 .84-.27 2.75 1.026A9.564 9.564 0 0112 6.844c.85.004 1.705.115 2.504.337 1.909-1.296 2.747-1.027 2.747-1.027.546 1.379.202 2.398.1 2.651.64.7 1.028 1.595 1.028 2.688 0 3.848-2.339 4.695-4.566 4.943.359.309.678.92.678 1.855 0 1.338-.012 2.419-.012 2.747 0 .268.18.58.688.482A10.019 10.019 0 0022 12.017C22 6.484 17.522 2 12 2z" clipRule="evenodd" />
                    </svg>
                  </a>
                  <a href="#" className="text-white hover:text-blue-100 transition-colors">
                    <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 24 24" aria-hidden="true">
                      <path d="M8.29 20.251c7.547 0 11.675-6.253 11.675-11.675 0-.178 0-.355-.012-.53A8.348 8.348 0 0022 5.92a8.19 8.19 0 01-2.357.646 4.118 4.118 0 001.804-2.27 8.224 8.224 0 01-2.605.996 4.107 4.107 0 00-6.993 3.743 11.65 11.65 0 01-8.457-4.287 4.106 4.106 0 001.27 5.477A4.072 4.072 0 012.8 9.713v.052a4.105 4.105 0 003.292 4.022 4.095 4.095 0 01-1.853.07 4.108 4.108 0 003.834 2.85A8.233 8.233 0 012 18.407a11.616 11.616 0 006.29 1.84" />
                    </svg>
                  </a>
                </div>
              </div>
            </div>
          </footer>
        </div>
      </body>
    </html>
  );
} 