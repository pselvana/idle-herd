import AppKit
import IdleHerdCore

/// Vector template artwork stays sharp at menu bar scale and follows macOS tint.
enum HerdMenuBarIcon {
    private static let images: [NSImage] = (0..<8).map { state in
        let right = MenuBarActivity.RightLlama(rawValue: state >> 1)!
        let image = NSImage(size: NSSize(width: right == .hidden ? 15 : 31, height: 18), flipped: false) { _ in
            NSColor.black.set()
            drawLlama(at: 0, filled: state & 1 != 0)
            if right != .hidden {
                let context = NSGraphicsContext.current!.cgContext
                context.saveGState()
                // Template images preserve alpha. Composite the whole silhouette
                // once so fill/stroke overlap does not darken the gray state's edge.
                context.setAlpha(right == .outgoing ? 0.5 : 1)
                context.beginTransparencyLayer(auxiliaryInfo: nil)
                drawLlama(at: 16, filled: right != .outline)
                context.endTransparencyLayer()
                context.restoreGState()
            }
            return true
        }
        image.isTemplate = true
        image.accessibilityDescription = right == .hidden ? "One llama" : "Two llamas shoulder to shoulder"
        return image
    }

    static func image(for activity: MenuBarActivity) -> NSImage {
        images[(activity.localExecuting ? 1 : 0) | (activity.rightLlama.rawValue << 1)]
    }

    private static func drawLlama(at offset: CGFloat, filled: Bool) {
        func point(_ x: CGFloat, _ y: CGFloat) -> NSPoint { NSPoint(x: x + offset, y: y) }
        let outline = NSBezierPath()
        outline.move(to: point(1, 1))
        outline.curve(to: point(2.5, 6.4), controlPoint1: point(1, 3.5), controlPoint2: point(1.6, 5))
        outline.line(to: point(3, 7.2))
        outline.curve(to: point(3, 11.2), controlPoint1: point(0.7, 8.6), controlPoint2: point(1.2, 10.1))
        outline.curve(to: point(3.2, 16.4), controlPoint1: point(2.3, 13.2), controlPoint2: point(2.2, 15.5))
        outline.curve(to: point(6.2, 12.8), controlPoint1: point(4.5, 18.3), controlPoint2: point(6.2, 16.2))
        outline.curve(to: point(8.8, 12.8), controlPoint1: point(7, 13.2), controlPoint2: point(8, 13.2))
        outline.curve(to: point(11.8, 16.4), controlPoint1: point(8.8, 16.2), controlPoint2: point(10.5, 18.3))
        outline.curve(to: point(12, 11.2), controlPoint1: point(12.8, 15.5), controlPoint2: point(12.7, 13.2))
        outline.curve(to: point(12, 7.2), controlPoint1: point(13.8, 10.1), controlPoint2: point(14.3, 8.6))
        outline.line(to: point(12.5, 6.4))
        outline.curve(to: point(14, 1), controlPoint1: point(13.4, 5), controlPoint2: point(14, 3.5))
        if filled {
            let silhouette = outline.copy() as! NSBezierPath
            silhouette.close()
            silhouette.fill()
        }
        outline.lineWidth = 1.5
        outline.lineCapStyle = .round
        outline.lineJoinStyle = .round
        outline.stroke()
    }
}
