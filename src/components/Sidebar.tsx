
import React from 'react';
import { 
  Database, 
  Upload, 
  BarChart3, 
  Search, 
  Settings, 
  Grid3X3, 
  FileText,
  Brain,
  TestTube
} from 'lucide-react';
import { cn } from '@/lib/utils';

interface SidebarProps {
  activeSection: string;
  onSectionChange: (section: string) => void;
}

const Sidebar = ({ activeSection, onSectionChange }: SidebarProps) => {
  const menuItems = [
    { id: 'data-mapping', label: 'Data Mapping', icon: Database },
    { id: 'lineage', label: 'Lineage', icon: Upload },
    { id: 'metadata', label: 'Metadata', icon: FileText },
    { id: 'test-generator', label: 'Test Data Generator', icon: TestTube },
    { id: 'ai-assistant', label: 'AI Assistant', icon: Brain },
    { id: 'manage-tables', label: 'Manage Tables', icon: Grid3X3 },
    { id: 'manage-columns', label: 'Manage Columns', icon: BarChart3 },
    { id: 'search', label: 'Search Business Metadata', icon: Search },
    { id: 'settings', label: 'Settings', icon: Settings },
  ];

  return (
    <div className="w-64 bg-slate-900 text-white h-screen fixed left-0 top-0 overflow-y-auto">
      <div className="p-6">
        <h1 className="text-xl font-bold text-white mb-8">Data Mapping Platform</h1>
        
        <nav className="space-y-2">
          {menuItems.map((item) => {
            const Icon = item.icon;
            return (
              <button
                key={item.id}
                onClick={() => onSectionChange(item.id)}
                className={cn(
                  "w-full flex items-center space-x-3 px-4 py-3 rounded-lg text-left transition-colors",
                  activeSection === item.id
                    ? "bg-blue-600 text-white"
                    : "text-slate-300 hover:bg-slate-800 hover:text-white"
                )}
              >
                <Icon size={20} />
                <span className="text-sm font-medium">{item.label}</span>
              </button>
            );
          })}
        </nav>
      </div>
    </div>
  );
};

export default Sidebar;
