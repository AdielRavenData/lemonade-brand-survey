import pandas as pd
import re
import logging
from pathlib import Path

class CustomSurveyDataCleaner:
    """Clean messy open-ended survey responses following KISS principle"""
    
    def __init__(self):
        # Brand mapping for cleaning (insurance focused)
        self.brand_patterns = {
            # Major insurance brands with real-world variations
            'State Farm': [
                'state farm', 'statefarm', 'state farms', 'statefarms', 'state farn',
                'state fram', 'state fatm', 'state farme', 'state farmm', 'state darm',
                'staye farm', 'stte farm', 'stat farm', 'state form', 'state far',
                'st farm', 'st farms', 'statefarm insurance', 'state farm ', 'state farm',
                'State farm', 'State Farm', 'State Farm ', 'State farm ', 'state farm'
            ],
            'Geico': [
                'geico', 'gieco', 'gico', 'gyco', 'gecko', 'geco', 'gecio', 'giego', 
                'gieko', 'geigo', 'geicho', 'geicko', 'geicoo', 'giceo', 'gicko', 'gigo',
                'gaico', 'giecco', 'geico insurance', 'Geico', 'Geico ', 'GEICO', 'GEICO ',
                'geico', 'geico ', 'Gieco', 'Gieco ', 'Gico', 'Gico ', 'gieco', 'gieco ',
                'geico', 'geico ', 'gico', 'gico ', ' Geico', 'Geico.', 'Geico?', 'Geicoo',
                'GIECO', 'GIECO ', 'Geico\'s'
            ],
            'Progressive': [
                'progressive', 'progessive', 'proggresive', 'progresive', 'proggressive', 
                'pergressive', 'progrssive', 'progreessive', 'prgressiv', 'progressiv',
                'progress', 'prlogressive', 'pergresive', 'prgressive', 'Progressive ',
                'progressive', 'progressive ', 'Progress', 'Progress '
            ],
            'Allstate': [
                'allstate', 'all state', 'all states', 'alstate', 'alsate', 'allstare',
                'allstaye', 'alsrate', 'alsstate', 'allstarte', 'allstated', 'alllstate',
                'all stat', 'allstars', 'allstar', 'Allstate', 'Allstate ', 'All state',
                'All state ', 'All State', 'All State ', 'Alstate', 'Alstate ', 'allstate',
                'allstate ', 'Allstate', 'Allstate '
            ],
            'Liberty Mutual': [
                'liberty', 'liberty mutual', 'librety', 'liberty mitchell', 'liberdy',
                'lirbety', 'liberty neutral', 'liberity', 'libery', 'bibrety', 'Liberty',
                'Liberty ', 'Liberty mutual', 'Liberty mutual ', 'Liberty Mutual',
                'Liberty Mutual '
            ],
            'Farmers': [
                'farmers', 'farm bureau', 'farmer', 'farm burau', 'farm bearu',
                'farm beaura', 'farm beuro', 'farm buro', 'farm beura', 'farm bearo',
                'farm bearue', 'farm beaure', 'Farmers', 'Farmers ', 'Farm bureau',
                'Farm bureau '
            ],
            'USAA': [
                'usaa', 'ussa', 'Usaa', 'USAA', 'USAA '
            ],
            'Lemonade': [
                'lemonade', 'lemona', 'lemonde', 'lemonaid', 'Lemonade', 'Lemonade ',
                'lemonade', 'lemonade ', 'LEMONADE', 'LEMONADE ', 'Lemonad', 'Lemonad3',
                'Lemonadr', 'Lemonades', 'Lemonada', 'Lemonade pet insurance',
                'Lemonade pet insurance ', 'Lemonade pet', 'Lemonade Pet Insurance',
                'Lemonade Pet Insurance ', 'Pet lemonade', 'Lemonade insurance',
                'Lemonade insurance ', 'Lemonade?'
            ],
            'The General': [
                'general', 'the general', 'General', 'General ', 'The general',
                'The general ', 'The General', 'The General '
            ],
            'Nationwide': [
                'nationwide', 'nation wide', 'Nationwide', 'Nationwide '
            ],
            'Travelers': [
                'travelers', 'travellers'
            ],
            'American Family': [
                'american family', 'am fam', 'amfam'
            ],
            'Root': [
                'root', 'root insurance', 'Root'
            ],
            'Metromile': [
                'metromile', 'metro mile'
            ],
            'Clearcover': [
                'clearcover', 'clear cover'
            ],
            'Blue Cross Blue Shield': [
                'blue cross', 'bcbs', 'blue cross blue shield'
            ],
            'Humana': [
                'humana'
            ],
            'Aetna': [
                'aetna', 'Aetna'
            ],
            'Cigna': [
                'cigna'
            ],
            'UnitedHealth': [
                'united', 'united health', 'unitedhealthcare'
            ],
            'Esurance': [
                'esurance', 'e-surance', 'Esurance', 'Esurance '
            ],
            'Safe Auto': [
                'safe auto', 'safeauto'
            ],
            'Direct Auto': [
                'direct auto', 'direct'
            ],
            'Endurance': [
                'endurance'
            ],
            'Aflac': [
                'aflac', 'Aflac'
            ],
            'Shelter': [
                'shelter', 'shelter insurance'
            ],
            'Erie': [
                'erie', 'erie insurance', 'Erie'
            ],
            'AARP': [
                'aarp', 'Aarp'
            ],
            'Hartford': [
                'hartford', 'the hartford'
            ],
            'Prudential': [
                'prudential'
            ],
            'Auto-Owners': [
                'auto owners', 'auto-owners', 'autoowners'
            ],
            'Western & Southern': [
                'western and southern', 'western southern', 'western & southern'
            ],
            'Mutual of Omaha': [
                'mutual of omaha', 'mutual omaha'
            ],
            'Gerber': [
                'gerber', 'gerber life'
            ],
            'Safeco': [
                'safeco', 'safeco insurance', 'Safeco'
            ],
            'Grange': [
                'grange', 'grange insurance', 'grange mutual'
            ],
            'Otto': [
                'otto', 'otto insurance', 'Otto'
            ],
            'NJM': [
                'njm', 'new jersey manufacturers', 'njm insurance'
            ],
            'Anthem': [
                'anthem', 'anthem insurance', 'anthem blue cross', 'anthem bcbs'
            ],
            'Fred Loya': [
                'fred loya', 'fredloya', 'fred loya insurance'
            ],
            'Pronto': [
                'pronto', 'pronto insurance'
            ],
            'Elephant': [
                'elephant', 'elephant insurance'
            ],
            'Zebra': [
                'zebra', 'zebra insurance'
            ],
            'Amica': [
                'amica', 'amica insurance', 'amica mutual'
            ]
        }
        
        # Non-answer patterns
        self.non_answer_patterns = [
            # Direct negatives (from real data)
            'no', 'none', 'nothing', 'na', 'n/a', 'nada', 'nope', 'n a', 'no e',
            'No', 'None', 'Nothing', 'Na', 'Na ', 'None ', 'Nothing ', 'No ',
            'none', 'nothing', 'Non', 'Non ', '0', '1', '0 ', '1 ',
            
            # Don't know variations (MAJOR CATEGORY - from real data)
            'dont know', "don't know", 'don t know', 'dont know any', 'don t know any',
            'idk', 'i dont know', "i don't know", 'i don t know', 'i dont know any', 'i don t know any',
            'i don t', 'i dont', 'dont', 'not sure', 'no idea', 'unknown', 'unsure', 'dunno', 'no clue',
            'Idk', 'Idk ', 'I don\'t know', 'I don\'t know ', 'I dont', 'I dont ',
            'Don\'t know', 'Don\'t know ', 'Don\'t know any', 'Don\'t know any ',
            'Not sure', 'Not sure ', 'No idea', 'No idea ', 'Unknown', 'Unknown ',
            'I don\'t', 'I don\'t ', 'I don\'t know ', 'I don\'t know',
            
            # Other non-responses (from real data)
            'not interested', 'no one', 'not any', 'cant think', 'no brand',
            'dont care', 'don t care', 'dont use', 'not much', 'not now', 'never',
            'Never', 'Never ', 'Ok', 'Ok ', 'Yes', 'Yes ', 'Yes', 'Yes ',
            'Hi', 'Hi ', 'Car', 'Car ', 'Life', 'Life ', 'Scam', 'Scam ',
            
            # Empty/whitespace variations
            '', ' ', '  ', '   ', '.', '. ', '?', '? '
        ]
    
    def clean_brand_response(self, response):
        """Clean a single brand response"""
        if pd.isna(response) or response == '':
            return 'None/Unknown'
        
        # Convert to string and basic cleaning
        response = str(response).strip().lower()
        
        # Remove extra punctuation and normalize spaces
        response = re.sub(r'[^\w\s]', ' ', response)
        response = ' '.join(response.split())  # Remove extra whitespace
        
                    # Check for non-answers first (but be careful with partial matches)
        for pattern in self.non_answer_patterns:
            if response == pattern:  # Exact match for non-answers
                return 'None/Unknown'
        
        # Check for brand matches
        for brand, patterns in self.brand_patterns.items():
            for pattern in patterns:
                if pattern in response:
                    return brand
        
        # Handle partial/incomplete brand names (only if response is short)
        if len(response) <= 5:  # Only for very short responses to avoid false matches
            partial_matches = {
                'farm': 'State Farm',
                'state': 'State Farm', 
                'pro': 'Progressive',
                'prog': 'Progressive',
                'gei': 'Geico',
                'all': 'Allstate',
                'lib': 'Liberty Mutual',
                'gen': 'The General'
            }
            
            for partial, full_brand in partial_matches.items():
                if response == partial:  # Exact match for short responses
                    return full_brand
        
        # Special handling for clearly non-insurance responses
        non_insurance_patterns = [
            'nike', 'amazon', 'apple', 'google', 'facebook', 'microsoft',
            'walmart', 'target', 'mcdonalds', 'starbucks', 'coca cola',
            'pepsi', 'ford', 'toyota', 'honda', 'chevrolet', 'bmw',
            'obamacare', 'medicare', 'medicaid', 'social security',
            # New non-insurance patterns from batch analysis
            'zara', 'iran', 'lemon', 'nine', 'metropolitan', 'the duck one',
            'i love', 'bee'
        ]
        
        for pattern in non_insurance_patterns:
            if pattern in response:
                return 'Non-Insurance Response'
        
        # If no match, return cleaned version for manual review
        return response.title()
    
    def split_multiple_brands(self, response):
        """Split multiple brand mentions in Q2 responses"""
        if pd.isna(response) or response == '':
            return ['None/Unknown']
        
        response = str(response).strip()
        
        # Common separators users use
        separators = [',', '.', ';', ' and ', '&', ' & ', ' + ', '|']
        
        # Replace all separators with a common one
        for sep in separators:
            response = response.replace(sep, '|')
        
        # Split and clean each part
        brands = []
        for brand in response.split('|'):
            brand = brand.strip()
            if brand:  # Skip empty strings
                cleaned_brand = self.clean_brand_response(brand)
                if cleaned_brand != 'None/Unknown':  # Skip non-answers in lists
                    brands.append(cleaned_brand)
        
        # Return None/Unknown if no valid brands found
        return brands if brands else ['None/Unknown']
    
    def clean_custom_data(self, df):
        """Clean custom survey data with open-ended questions"""
        df_clean = df.copy()
        
        # Find the question columns
        q1_col = None
        q2_col = None
        
        for col in df.columns:
            if 'OPEN_ENDED' in col and 'first brand' in col:
                q1_col = col
            elif 'OPEN_ENDED' in col and 'other' in col:
                q2_col = col
        
        if not q1_col or not q2_col:
            logging.warning("Could not find Q1 and Q2 columns")
            return df_clean
        
        logging.info(f"Cleaning Q1: {q1_col[:50]}...")
        logging.info(f"Cleaning Q2: {q2_col[:50]}...")
        
        # Clean Q1 (single brand expected)
        df_clean['q1_cleaned'] = df_clean[q1_col].apply(self.clean_brand_response)
        
        # Clean Q2 (multiple brands possible) - keep as string for now
        df_clean['q2_cleaned'] = df_clean[q2_col].apply(
            lambda x: '; '.join(self.split_multiple_brands(x))
        )
        
        return df_clean
    
    def explode_q2_brands(self, df):
        """Create exploded version of Q2 for analysis (like brand tracker)"""
        if 'q2_cleaned' not in df.columns:
            return df
        
        df_exploded = df.copy()
        
        # Split Q2 cleaned brands and explode
        df_exploded['q2_individual'] = df_exploded['q2_cleaned'].str.split('; ')
        df_exploded = df_exploded.explode('q2_individual')
        
        # Remove None/Unknown entries for cleaner analysis
        df_exploded = df_exploded[df_exploded['q2_individual'] != 'None/Unknown']
        
        return df_exploded.reset_index(drop=True)
    
    def generate_cleaning_report(self, df_original, df_cleaned):
        """Generate a report showing cleaning results"""
        q1_col = None
        q2_col = None
        
        for col in df_original.columns:
            if 'OPEN_ENDED' in col and 'first brand' in col:
                q1_col = col
            elif 'OPEN_ENDED' in col and 'other' in col:
                q2_col = col
        
        if not q1_col or not q2_col:
            return "Could not find question columns"
        
        report = []
        report.append("=== CUSTOM SURVEY CLEANING REPORT ===")
        report.append(f"Total responses processed: {len(df_original)}")
        report.append("")
        
        # Q1 Analysis
        report.append("=== Q1 (First Brand) - BEFORE CLEANING ===")
        q1_before = df_original[q1_col].value_counts().head(15)
        for brand, count in q1_before.items():
            report.append(f"{brand}: {count}")
        
        report.append("")
        report.append("=== Q1 (First Brand) - AFTER CLEANING ===")
        q1_after = df_cleaned['q1_cleaned'].value_counts().head(15)
        for brand, count in q1_after.items():
            report.append(f"{brand}: {count}")
        
        report.append("")
        report.append("=== Q2 (Other Brands) - BEFORE CLEANING ===")
        q2_before = df_original[q2_col].value_counts().head(15)
        for brand, count in q2_before.items():
            report.append(f"'{brand}': {count}")
        
        report.append("")
        report.append("=== Q2 (Other Brands) - AFTER CLEANING ===")
        q2_after = df_cleaned['q2_cleaned'].value_counts().head(15)
        for brand, count in q2_after.items():
            report.append(f"'{brand}': {count}")
        
        return "\n".join(report)
