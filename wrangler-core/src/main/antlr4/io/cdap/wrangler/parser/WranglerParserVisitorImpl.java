public class WranglerParserVisitorImpl extends WranglerParserBaseVisitor<Void> {
    
    @Override
    public Void visitByteSize(WranglerParser.ByteSizeContext ctx) {
        // Your logic for visiting the ByteSize node
        return visitChildren(ctx);
    }

    @Override
    public Void visitTimeSize(WranglerParser.TimeSizeContext ctx) {
        // Your logic for visiting the TimeSize node
        return visitChildren(ctx);
    }

    // Other visit methods for different nodes...
}
